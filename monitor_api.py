import requests
import pandas as pd
import os
import time
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import logging
from datetime import datetime
import traceback
import tkinter as tk
from tkinter import filedialog, messagebox
import sys
import json
import threading

# Configuration file path
CONFIG_FILE = "monitor_config.json"

def load_config():
    """Load configuration from file if it exists"""
    if os.path.exists(CONFIG_FILE):
        try:
            with open(CONFIG_FILE, 'r') as f:
                return json.load(f)
        except Exception as e:
            print(f"Error loading config: {str(e)}")
    return None

def save_config(config):
    """Save configuration to file"""
    try:
        with open(CONFIG_FILE, 'w') as f:
            json.dump(config, f, indent=4)
    except Exception as e:
        print(f"Error saving config: {str(e)}")

def get_user_paths():
    """Get paths from user input using GUI dialogs"""
    # First try to load existing config
    config = load_config()
    if config:
        return config['pdf_folder'], config['base_output_folder'], config['log_file']
    
    try:
        # Initialize tkinter
        root = tk.Tk()
        root.withdraw()  # Hide the main window
        
        # Ensure the window appears on top
        root.attributes('-topmost', True)
        
        # Show welcome message
        messagebox.showinfo("PDF Monitor Setup", 
                          "Welcome to PDF Monitor!\n\n"
                          "You will be asked to select three locations:\n"
                          "1. Folder to monitor for PDF files\n"
                          "2. Folder to save Excel outputs\n"
                          "3. Location to save the log file\n\n"
                          "Click OK to begin.")
        
        # Get PDF folder path
        pdf_folder = filedialog.askdirectory(
            title="Select Folder to Monitor for PDF Files",
            mustexist=True
        )
        if not pdf_folder:
            raise ValueError("PDF folder selection is required")
        
        # Get base output folder
        base_output_folder = filedialog.askdirectory(
            title="Select Folder to Save Excel Outputs",
            mustexist=True
        )
        if not base_output_folder:
            raise ValueError("Output folder selection is required")
        
        # Get log file path
        log_file = filedialog.asksaveasfilename(
            title="Save Log File",
            defaultextension=".txt",
            filetypes=[("Text files", "*.txt"), ("All files", "*.*")],
            initialdir=os.path.dirname(base_output_folder)
        )
        if not log_file:
            raise ValueError("Log file location is required")
        
        # Save configuration
        config = {
            'pdf_folder': pdf_folder,
            'base_output_folder': base_output_folder,
            'log_file': log_file
        }
        save_config(config)
        
        # Show confirmation
        messagebox.showinfo("Setup Complete", 
                          f"Setup complete!\n\n"
                          f"PDF Monitor Folder: {pdf_folder}\n"
                          f"Excel Output Folder: {base_output_folder}\n"
                          f"Log File: {log_file}\n\n"
                          f"Click OK to start monitoring.")
        
        return pdf_folder, base_output_folder, log_file
        
    except Exception as e:
        error_msg = f"Error during setup: {str(e)}"
        messagebox.showerror("Setup Error", error_msg)
        print(error_msg)
        sys.exit(1)
    finally:
        try:
            root.destroy()
        except:
            pass

# Configuration
try:
    PDF_FOLDER_PATH, BASE_OUTPUT_FOLDER, LOG_FILE = get_user_paths()
except Exception as e:
    print(f"Error getting paths: {str(e)}")
    sys.exit(1)

DOCUMENT_EXTRACT_API_URL = "http://localhost:8080/extract/"
ENTITY_EXTRACTOR_API_URL = "http://localhost:8080/classification"
# API_KEY = "mock-api-key"  # Replace with your API key
MAPPING_API_URL = "http://localhost:8080/mapping/get-mappings"
SAP_PURCHASE_API_URL = "http://localhost:8080/sap/PurchaseInvoices"

# Set up logging
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger()

# Log the configuration
logger.info(f"PDF Monitor Folder: {PDF_FOLDER_PATH}")
logger.info(f"Base Output Folder: {BASE_OUTPUT_FOLDER}")
logger.info(f"Log File: {LOG_FILE}")

# Step 1: Call Document Extract API
def call_document_extract_api(pdf_path):
    start_time = time.time()
    logger.info(f"Starting Document Extract API call for {pdf_path}")
    
    # headers = {
    #     "Authorization": f"Bearer {API_KEY}",
    #     "Accept": "application/json"
    # }
    
    try:
        with open(pdf_path, "rb") as pdf_file:
            files = {"file_list": (os.path.basename(pdf_path), pdf_file, "application/pdf")}
            data = {"prompt": ""}  # Add empty prompt
            response = requests.post(DOCUMENT_EXTRACT_API_URL, files=files)
            response.raise_for_status()
            response_time = time.time() - start_time
            logger.info(
                f"Document Extract API call successful for {pdf_path}. "
                f"Status: {response.status_code}, Response Time: {response_time:.2f}s"
            )
            logger.info(response.json())
            return response.json()
    except requests.exceptions.RequestException as e:
        response_time = time.time() - start_time
        logger.error(
            f"Document Extract API call failed for {pdf_path}. "
            f"Status: {getattr(e.response, 'status_code', 'N/A')}, "
            f"Response Time: {response_time:.2f}s, Error: {str(e)}"
        )
        logger.debug(f"Stack trace: {traceback.format_exc()}")
        return None

# Step 2: Call Entity Extractor API
# def call_entity_extractor_api(extracted_data, pdf_path):
#     start_time = time.time()
    
#     # Get document_id from the document extract response
#     document_id = extracted_data['data'][0]['document_id']
#     if not document_id:
#         logger.error(f"No document_id found in response for {pdf_path}")
#         return None
    
#     # Use document_id directly in the path
#     api_url = f"{ENTITY_EXTRACTOR_API_URL}/{document_id}"
#     logger.info(f"Starting Entity Extractor API call to {api_url}")
    
#     # headers = { ERROR - Error appending to Excel: 'UoMEntry'
#     #     "Authorization": f"Bearer {API_KEY}",
#     #     "Accept": "application/json"
#     # }
    
#     try:
#         response = requests.get(api_url)
#         response.raise_for_status()
#         response_time = time.time() - start_time
#         logger.info(
#             f"Entity Extractor API call successful. "
#             f"Status: {response.status_code}, Response Time: {response_time:.2f}s"
#         )
#         return response.json()
#     except requests.exceptions.RequestException as e:
#         response_time = time.time() - start_time
#         logger.error(
#             f"Entity Extractor API call failed. "
#             f"Status: {getattr(e.response, 'status_code', 'N/A')}, "
#             f"Response Time: {response_time:.2f}s, Error: {str(e)}"
#         )
#         logger.debug(f"Stack trace: {traceback.format_exc()}")
#         return None

def map_incoming_data(extracted_data):
    incoming_doc_id = extracted_data["data"][0]["document_id"]
    if not incoming_doc_id:
        logger.error("No document_id found in extracted data for mapping")

    api_url = f"{MAPPING_API_URL}/{incoming_doc_id}"
    raw_response = requests.get(api_url).json()
    mapped_data = raw_response['mapped_result']
    logger.info(mapped_data)
    return mapped_data

# Step 3: Process a single PDF
def process_pdf(pdf_path):
    logger.info(f"Processing new PDF: {pdf_path}")
    
    # Call Document Extract API
    doc_extract_response = call_document_extract_api(pdf_path)
    if not doc_extract_response:
        logger.error(f"Skipping {pdf_path} due to Document Extract API failure")
        return None
    
    # Call Entity Extractor API
    # entity_extract_response = call_entity_extractor_api(doc_extract_response, pdf_path)
    # if not entity_extract_response:
    #     logger.error(f"Skipping {pdf_path} due to Entity Extractor API failure")
    #     return None
    
    # Get classification from response
    # classification = entity_extract_response.get("classification", "").lower()
    # if not classification:
    #     logger.error(f"No classification found in response for {pdf_path}")
    #     return None
    
    mapped_response = map_incoming_data(doc_extract_response)
    
    # Add filename to the response data
    # entity_extract_response['file_name'] = os.path.basename(pdf_path)
    
    
    # If classification is not ap_invoice or outgoing_payment, set it to other_files
    # if classification not in ['ap_invoice', 'outgoing_payment']:
    #     classification = 'other_files'
    
    # Return the complete API response with classification
    return {
        # "classification": classification,  
        # "data": entity_extract_response,
        "mapped_data": mapped_response
    }

# Step 4: Append to Excel
def append_to_excel(processed_data, base_output_folder):
    try:
        if not processed_data:
            logger.warning("No data to save")
            return
        
        # Create daily folder with timestamp
        today = datetime.now().strftime("%Y-%m-%d")
        daily_folder = os.path.join(base_output_folder, f"excel_output_{today}")
        
        # Get classification and create classification-specific folder
        # classification = processed_data["classification"]
        classification_folder = os.path.join(daily_folder, "ap_invoice")
        os.makedirs(classification_folder, exist_ok=True)
        
        # Get the complete API response data
        api_data = processed_data["mapped_data"]
        
        # Build all DocumentLines (handle both UoMEntry and UomEntry keys)
        document_lines = []
        for line in api_data.get("DocumentLines", []):
            uom_entry = line.get("UoMEntry") or line.get("UomEntry") or line.get("uomentry")
            document_lines.append({
                "ItemCode": line.get("ItemCode", ""),
                "UoMEntry": uom_entry,
                "TaxCode": line.get("TaxCode", "")
            })
        
        invoice_data = {
            "CardCode": api_data.get("CardCode", ""),
            # "DocDate": api_data.get("DocDate", ""),
            "DocumentLines": document_lines
        }
        post_to_sap(invoice_data)
        # Create a dictionary to store all data
        flattened_data = {}
        
        # Function to recursively flatten nested dictionaries and lists
        def flatten_dict(d, parent_key='', sep='_'):
            items = []
            for k, v in d.items():
                new_key = f"{parent_key}{sep}{k}" if parent_key else k
                
                # Special handling for line_items
                if k == 'line_items' and isinstance(v, list):
                    # Flatten each line item into separate columns
                    for i, item in enumerate(v):
                        if isinstance(item, dict):
                            for item_k, item_v in item.items():
                                # Create a column for each line item field
                                line_item_key = f"line_item_{i+1}_{item_k}"
                                if isinstance(item_v, list):
                                    if all(isinstance(x, str) for x in item_v):
                                        items.append((line_item_key, '; '.join(item_v)))
                                    else:
                                        items.append((line_item_key, str(item_v)))
                                else:
                                    items.append((line_item_key, item_v))
                    continue
                
                if isinstance(v, dict):
                    items.extend(flatten_dict(v, new_key, sep=sep).items())
                elif isinstance(v, list):
                    # For lists, join with semicolon if they're strings, otherwise convert to string
                    if all(isinstance(item, str) for item in v):
                        items.append((new_key, '; '.join(v)))
                    else:
                        items.append((new_key, str(v)))
                else:
                    items.append((new_key, v))
            return dict(items)
        
        # Flatten the complete API response
        flattened_data = flatten_dict(api_data)
        
        # Create DataFrame
        df_new = pd.DataFrame([flattened_data])
        
        # Add empty rows for spacing
        spacing_rows = pd.DataFrame([{} for _ in range(2)])  # Add 2 empty rows
        df_new = pd.concat([df_new, spacing_rows], ignore_index=True)
        
        # Define output path
        output_path = os.path.join(classification_folder, "ap_invoice_output.xlsx")
        
        # If Excel file exists, append; otherwise, create new
        if os.path.exists(output_path):
            df_existing = pd.read_excel(output_path, engine="openpyxl")
            df_combined = pd.concat([df_existing, df_new], ignore_index=True)
        else:
            df_combined = df_new
        
        # Save to Excel
        df_combined.to_excel(output_path, index=False, engine="openpyxl")
        logger.info(f"Successfully appended data to {output_path}")
            
    except Exception as e:
        logger.error(f"Error appending to Excel: {str(e)}")
        logger.debug(f"Stack trace: {traceback.format_exc()}")

def post_to_sap(mapped_data):
    try:
        logger.info(f"Posting the following data: {mapped_data}")
        response = requests.post(SAP_PURCHASE_API_URL, json=mapped_data)
        response.raise_for_status()
        logger.info(f"Successfully posted data to SAP. Status: {response.status_code}")
        return response.json()
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to post data to SAP. Error: {str(e)}")
        logger.debug(f"Stack trace: {traceback.format_exc()}")
        return None

def get_current_day_folder(base_path):
    """Get the current day's folder path"""
    today = datetime.now().strftime("%Y-%m-%d")
    return os.path.join(base_path, today)

class DynamicFolderHandler(FileSystemEventHandler):
    def __init__(self, base_folder_path):
        self.base_folder_path = base_folder_path
        self.current_folder = get_current_day_folder(base_folder_path)
        self.processed_files = set()
        self.observer = None
        self.last_check_time = time.time()
        self.check_interval = 60  # Check for new day folder every minute
        
    def start_monitoring(self):
        """Start monitoring the current day's folder"""
        if not os.path.exists(self.current_folder):
            logger.warning(f"Current day folder {self.current_folder} does not exist. Waiting for it to be created...")
            return False
            
        self.observer = Observer()
        self.observer.schedule(self, self.current_folder, recursive=False)
        self.observer.start()
        logger.info(f"Started monitoring {self.current_folder}")
        return True
        
    def stop_monitoring(self):
        """Stop the current observer"""
        if self.observer:
            self.observer.stop()
            self.observer.join()
            self.observer = None
            
    def check_for_new_day(self):
        """Check if we need to switch to a new day's folder"""
        current_time = time.time()
        if current_time - self.last_check_time >= self.check_interval:
            self.last_check_time = current_time
            new_folder = get_current_day_folder(self.base_folder_path)
            
            if new_folder != self.current_folder and os.path.exists(new_folder):
                logger.info(f"New day folder detected: {new_folder}")
                self.stop_monitoring()
                self.current_folder = new_folder
                self.processed_files.clear()  # Clear processed files for new day
                return self.start_monitoring()
        return True

    def process_file(self, file_path):
        if not file_path.lower().endswith(".pdf"):
            return

        if not self.is_file_ready(file_path):
            return

        if file_path in self.processed_files:
            return

        self.processed_files.add(file_path)
        logging.info(f"New PDF detected (processed): {file_path}")

        processed_data = process_pdf(file_path)
        if processed_data:
            append_to_excel(processed_data, BASE_OUTPUT_FOLDER)

    def on_created(self, event):
        if event.is_directory:
            return
        self.process_file(event.src_path)

    def on_moved(self, event):
        if event.is_directory:
            return
        self.process_file(event.dest_path)

    def is_file_ready(self, file_path, timeout=30, check_interval=1):
        start_time = time.time()
        last_size = -1

        while time.time() - start_time < timeout:
            try:
                current_size = os.path.getsize(file_path)
                if current_size == last_size:
                    return True
                last_size = current_size
                time.sleep(check_interval)
            except (OSError, FileNotFoundError):
                return False

        logging.warning(f"Timeout waiting for {file_path} to be ready")
        return False
    

# Main Workflow
def main():
    # Verify base folder exists
    if not os.path.exists(PDF_FOLDER_PATH):
        error_msg = f"Base folder not found at {PDF_FOLDER_PATH}"
        logger.error(error_msg)
        messagebox.showerror("Error", error_msg)
        return
    
    # Set up dynamic folder handler
    event_handler = DynamicFolderHandler(PDF_FOLDER_PATH)
    
    # Try to start monitoring the current day's folder
    if not event_handler.start_monitoring():
        # If current day's folder doesn't exist, show a message
        messagebox.showinfo("Waiting for Folder", 
                          f"Waiting for today's folder to be created at:\n{event_handler.current_folder}\n\n"
                          f"The program will automatically start monitoring when the folder is created.")
    
    # Show monitoring status
    messagebox.showinfo("Monitoring Started", 
                       f"PDF Monitor is now running!\n\n"
                       f"Base folder: {PDF_FOLDER_PATH}\n"
                       f"Current monitoring folder: {event_handler.current_folder}\n"
                       f"Excel outputs will be saved in: {BASE_OUTPUT_FOLDER}\n"
                       f"Log file location: {LOG_FILE}\n\n"
                       f"Click OK to continue monitoring.\n"
                       f"The program will automatically switch to new day's folder when available.\n"
                       f"To stop monitoring, close this window and press Ctrl+C in the console.")
    
    try:
        while True:
            # Check for new day folder periodically
            event_handler.check_for_new_day()
            time.sleep(1)  # Keep the script running
    except KeyboardInterrupt:
        event_handler.stop_monitoring()
        logger.info("Stopped monitoring")
        messagebox.showinfo("Monitoring Stopped", "PDF Monitor has been stopped.")

if __name__ == "__main__":
    main()
