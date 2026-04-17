import logging
from datetime import datetime
from backend.services.query_utils import fetch_all, fetch_one
from backend.config import DATAMART_SCHEMA_NAME

logger = logging.getLogger(__name__)
DW = DATAMART_SCHEMA_NAME


def get_data_quality_report():
    """Return a comprehensive data quality report."""
    try:
        # Total records in fact_session
        total_res = fetch_one(f"SELECT COUNT(*) as count FROM {DW}.fact_session")
        total_records = int(total_res.get("count", 0))

        # Date range
        date_range = fetch_one(f"""
            SELECT MIN(d.full_date) as min_date, MAX(d.full_date) as max_date
            FROM {DW}.fact_session f
            JOIN {DW}.dim_date d ON f.date_id = d.date_id
        """)

        # Missing dates (days with no sessions in the date range)
        missing_dates_res = fetch_one(f"""
            SELECT COUNT(*) as count FROM (
                SELECT d.full_date
                FROM {DW}.dim_date d
                WHERE d.full_date BETWEEN (
                    SELECT MIN(d2.full_date) FROM {DW}.fact_session f2 JOIN {DW}.dim_date d2 ON f2.date_id = d2.date_id
                ) AND (
                    SELECT MAX(d2.full_date) FROM {DW}.fact_session f2 JOIN {DW}.dim_date d2 ON f2.date_id = d2.date_id
                )
                AND d.is_weekend = FALSE
                AND NOT EXISTS (
                    SELECT 1 FROM {DW}.fact_session f WHERE f.date_id = d.date_id
                )
            ) sub
        """)
        missing_dates = int(missing_dates_res.get("count", 0))

        # Active alerts count
        alerts_res = fetch_one(f"""
            SELECT COUNT(*) as count FROM {DW}.data_quality_alerts
            WHERE resolved_at IS NULL
        """)
        active_alerts = int(alerts_res.get("count", 0))

        # Daily session counts (for trend chart — last 90 days)
        daily_counts = fetch_all(f"""
            SELECT d.full_date::text as date, COUNT(f.sk_fact_session_id) as sessions
            FROM {DW}.dim_date d
            LEFT JOIN {DW}.fact_session f ON f.date_id = d.date_id
            WHERE d.full_date >= (SELECT MAX(d2.full_date) - INTERVAL '90 days' FROM {DW}.fact_session f2 JOIN {DW}.dim_date d2 ON f2.date_id = d2.date_id)
            AND d.full_date <= (SELECT MAX(d2.full_date) FROM {DW}.fact_session f2 JOIN {DW}.dim_date d2 ON f2.date_id = d2.date_id)
            GROUP BY d.full_date
            ORDER BY d.full_date
        """)

        # Detect data drops (> 50% decrease from 7-day rolling average)
        drops = []
        if len(daily_counts) > 7:
            for i in range(7, len(daily_counts)):
                window = [daily_counts[j]["sessions"] for j in range(i - 7, i)]
                avg = sum(window) / len(window)
                current = daily_counts[i]["sessions"]
                if avg > 5 and current < avg * 0.5:
                    drops.append({
                        "date": daily_counts[i]["date"],
                        "sessions": current,
                        "rolling_avg": round(avg, 1),
                        "drop_pct": round((1 - current / avg) * 100, 1)
                    })

        return {
            "kpis": {
                "total_records": total_records,
                "missing_dates": missing_dates,
                "active_alerts": active_alerts,
                "date_range": f"{date_range.get('min_date', 'N/A')} to {date_range.get('max_date', 'N/A')}"
            },
            "daily_counts": [{"label": r["date"], "value": r["sessions"]} for r in daily_counts],
            "drops": drops
        }
    except Exception as e:
        logger.error(f"Error in data quality report: {e}", exc_info=True)
        return {
            "kpis": {"total_records": 0, "missing_dates": 0, "active_alerts": 0, "date_range": "N/A"},
            "daily_counts": [],
            "drops": []
        }


def get_recent_alerts(limit=50):
    """Fetch recent data quality alerts."""
    try:
        return fetch_all(f"""
            SELECT alert_id, alert_type, description, severity, detected_at::text, resolved_at::text, metadata
            FROM {DW}.data_quality_alerts
            ORDER BY detected_at DESC
            LIMIT %s
        """, [limit])
    except Exception as e:
        logger.error(f"Error fetching alerts: {e}", exc_info=True)
        return []


def store_alert(alert_type, description, severity="warning", metadata=None):
    """Store a data quality alert in the database."""
    try:
        from backend.db import get_datamart_conn
        import json
        with get_datamart_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(f"""
                    INSERT INTO {DW}.data_quality_alerts (alert_type, description, severity, metadata)
                    VALUES (%s, %s, %s, %s)
                """, [alert_type, description, severity, json.dumps(metadata) if metadata else None])
            conn.commit()
    except Exception as e:
        logger.error(f"Error storing alert: {e}", exc_info=True)


def run_data_quality_check():
    """Run a comprehensive data quality check and store alerts for issues found."""
    try:
        report = get_data_quality_report()

        # Store alerts for data drops
        for drop in report.get("drops", []):
            store_alert(
                alert_type="data_drop",
                description=f"Session count dropped to {drop['sessions']} on {drop['date']} (rolling avg: {drop['rolling_avg']}, drop: {drop['drop_pct']}%)",
                severity="critical" if drop["drop_pct"] > 80 else "warning",
                metadata=drop
            )

        if report["kpis"]["missing_dates"] > 10:
            store_alert(
                alert_type="missing_data",
                description=f"{report['kpis']['missing_dates']} weekdays with no session data found in the date range",
                severity="warning",
                metadata={"missing_count": report["kpis"]["missing_dates"]}
            )

        return {"status": "completed", "drops_found": len(report.get("drops", [])), "missing_dates": report["kpis"]["missing_dates"]}
    except Exception as e:
        logger.error(f"Error in data quality check: {e}", exc_info=True)
        return {"status": "error", "message": str(e)}
