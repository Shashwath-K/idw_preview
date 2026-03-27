import io
import pandas as pd
from fastapi.responses import StreamingResponse

def json_to_excel_streaming_response(data: list[dict], filename: str) -> StreamingResponse:
    """
    Converts a list of dictionaries to an Excel file and returns a StreamingResponse.
    """
    df = pd.DataFrame(data)
    
    # Create an in-memory buffer
    buffer = io.BytesIO()
    
    # Write the DataFrame to the buffer as an Excel file
    with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='Report')
    
    # Reset buffer position to the beginning
    buffer.seek(0)
    
    # Create the streaming response
    headers = {
        'Content-Disposition': f'attachment; filename="{filename}"'
    }
    return StreamingResponse(
        buffer, 
        media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        headers=headers
    )
