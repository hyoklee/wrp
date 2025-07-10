import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from abaqus import *
from abaqusConstants import *

def odb_to_parquet(odb_file_path, parquet_file_path, step_name, frame_number, variable_name):
    try:
        # Open the ODB file
        odb = openOdb(path=odb_file_path)
        
        # Access the specified step and frame
        step = odb.steps[step_name]
        frame = step.frames[frame_number]
        
        # Extract field output data
        field_output = frame.fieldOutputs[variable_name]
        
        # Prepare data for DataFrame
        data = []
        for value in field_output.values:
            node = value.node
            data.append({
                'Node': node.label,
                'X': node.coordinates[0],
                'Y': node.coordinates[1],
                'Z': node.coordinates[2],
                'Value': value.data[0]  # Assuming single component output
            })
        
        # Create a Pandas DataFrame
        df = pd.DataFrame(data)
        
        # Convert DataFrame to Parquet
        table = pa.Table.from_pandas(df)
        pq.write_table(table, parquet_file_path)
        
        print(f"Data successfully written to {parquet_file_path}")
    
    except Exception as e:
        print(f"An error occurred: {e}")
    
    finally:
        # Close the ODB
        if 'odb' in locals() and odb:
            odb.close()

# Example usage
odb_file_path = "path/to/your/output.db"
parquet_file_path = "output.parquet"
step_name = "Step-1"
frame_number = 0
variable_name = "U"  # Example variable name

odbc_to_parquet(odb_file_path, parquet_file_path, step_name, frame_number, variable_name)

