import os
import json
import pandas as pd

def combine_json_to_excel(root_dir, output_file):
    """
    Walks through a directory, finds all .json files within its subdirectories,
    reads their content, combines them into a single Excel file, and reports on the process.

    Args:
        root_dir (str): The path to the directory to search within.
        output_file (str): The path for the combined output Excel file.
    """
    combined_data = []
    failed_files = []
    dirs_without_json = []
    total_json_files_found = 0
    total_dirs_scanned = 0
    
    print(f"Starting to scan directory: {root_dir}")

    # Walk through the root directory to find subdirectories
    for item in os.listdir(root_dir):
        item_path = os.path.join(root_dir, item)
        if os.path.isdir(item_path):
            total_dirs_scanned += 1
            json_found_in_dir = False
            # Look for a JSON file inside the subdirectory
            for filename in os.listdir(item_path):
                if filename.endswith('.json'):
                    json_found_in_dir = True
                    total_json_files_found += 1
                    file_path = os.path.join(item_path, filename)
                    try:
                        with open(file_path, 'r', encoding='utf-8') as f:
                            data = json.load(f)
                            combined_data.append(data)
                    except json.JSONDecodeError as e:
                        print(f"Error decoding JSON from file: {file_path} - {e}")
                        failed_files.append(file_path)
                    except Exception as e:
                        print(f"An error occurred while reading file: {file_path} - {e}")
                        failed_files.append(file_path)
            
            if not json_found_in_dir:
                print(f"Warning: No JSON file found in directory: {item_path}")
                dirs_without_json.append(item_path)

    # --- Reporting ---
    successful_count = len(combined_data)
    failed_count = len(failed_files)

    print("\n--- Processing Report ---")
    print(f"Total directories scanned: {total_dirs_scanned}")
    print(f"Total JSON files found: {total_json_files_found}")
    print(f"Successfully processed files: {successful_count}")
    print(f"Failed to process files: {failed_count}")

    if failed_files:
        print("\nPaths of files that could not be processed:")
        for path in failed_files:
            print(f" - {path}")

    if dirs_without_json:
        print("\nDirectories that did not contain a .json file:")
        for path in dirs_without_json:
            print(f" - {path}")

    if not combined_data:
        print("\nNo data was successfully processed. The Excel file will not be created.")
        return

    # Convert the list of dictionaries to a pandas DataFrame
    df = pd.DataFrame(combined_data)

    # Write the DataFrame to an Excel file, without the pandas index
    df.to_excel(output_file, index=False)
    
    print(f"\nProcess complete. Combined {successful_count} JSON records into {output_file}")

if __name__ == "__main__":
    # Directory containing the ID-named subfolders with JSON files
    source_directory = r'd:\POCs\county_scrapping POC\county-scrapping\json_to_excel\output_lis 2\output_lis'
    # The final combined Excel file
    output_excel_path = r'd:\POCs\county_scrapping POC\county-scrapping\json_to_excel\output_lis 2\output_lis.xlsx'
    combine_json_to_excel(source_directory, output_excel_path)