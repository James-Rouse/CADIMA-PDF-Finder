import pandas as pd
import requests
import os
from urllib.parse import quote
from tqdm import tqdm
import logging

# Configure logging
logging.basicConfig(
    filename='cadima_pdf_finder.log',
    filemode='w',
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def read_references(file_path):
    try:
        df = pd.read_excel(file_path, engine="openpyxl")
        logging.info(f"Total rows in Excel file: {len(df)}")
        logging.info(f"Columns in Excel file: {df.columns.tolist()}")

        # Find DOI column (case-insensitive)
        doi_column = next((col for col in df.columns if 'doi' in col.strip().lower()), None)
        
        if not doi_column:
            logging.error("No 'DOI' column found in the Excel file.")
            return df, [], []
        
        logging.info(f"Using column '{doi_column}' for DOIs")
        
        # Clean and validate DOIs
        dois = (df[doi_column]
               .astype(str)
               .str.strip()
               .str.extract(r'(10\.\d{4,}[/.].+)', expand=False)  # Extract valid DOI pattern
               .dropna())
        
        # Extract 'Link to PDF' column and clean it
        link_to_pdf = df.get('Link to PDF', pd.Series([None]*len(df)))
        pdf_links = []
        for link in link_to_pdf[dois.index]:
            if pd.isna(link) or not isinstance(link, str):
                pdf_links.append(None)
            else:
                # Clean and validate URL
                link = link.strip()
                if link and (link.startswith('http://') or link.startswith('https://')):
                    pdf_links.append(link)
                else:
                    pdf_links.append(None)
        
        logging.info(f"Number of valid DOIs after cleaning: {len(dois)}")
        logging.debug(f"First few DOIs:\n{dois[:5].tolist()}")
        logging.debug(f"First few PDF links:\n{pdf_links[:5]}")
        
        return df, dois.tolist(), pdf_links
    except Exception as e:
        logging.error(f"Error reading Excel file: {str(e)}")
        return pd.DataFrame(), [], []

def search_unpaywall(doi):
    url = f"https://api.unpaywall.org/v2/{quote(doi)}"  # URL encode the DOI
    params = {'email': 'james.thomas.rouse@gmail.com'}  # Replace with your actual email
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
    
    try:
        response = requests.get(url, params=params, headers=headers, timeout=10)
        logging.debug(f"Unpaywall response for DOI {doi}: {response.status_code}")
        logging.debug(f"Unpaywall response content for DOI {doi}: {response.content}")
        
        if response.status_code == 200:
            data = response.json()
            logging.debug(f"Unpaywall response data for DOI {doi}: {data}")  # Added for debugging
            if data.get('is_oa'):
                # Try multiple URL sources
                pdf_url = (
                    data.get('best_oa_location', {}).get('url_for_pdf') or
                    data.get('best_oa_location', {}).get('url') or
                    next((loc.get('url_for_pdf') or loc.get('url') 
                         for loc in data.get('oa_locations', []) 
                         if loc.get('url_for_pdf') or loc.get('url')), 
                         None)
                )
                if pdf_url:
                    logging.info(f"Found PDF URL for DOI {doi}: {pdf_url}")
                    return pdf_url
                else:
                    logging.debug(f"No PDF URL found in Unpaywall response for DOI {doi}")
            else:
                logging.debug(f"DOI {doi} is not open access according to Unpaywall")
        else:
            logging.warning(f"Unpaywall response for DOI {doi} returned status code {response.status_code}")
    except Exception as e:
        logging.error(f"Unpaywall error for {doi}: {str(e)}")
    return None

def search_pubmed(doi):
    url = "https://api.ncbi.nlm.nih.gov/lit/ctxp/v1/pmc/"
    try:
        response = requests.get(f"{url}{doi}")
        logging.debug(f"PubMed response for DOI {doi}: {response.status_code}")
        logging.debug(f"PubMed response content for DOI {doi}: {response.content}")
        
        if response.status_code == 200:
            data = response.json()
            logging.debug(f"PubMed response data for DOI {doi}: {data}")  # Added for debugging
            pdf_url = data.get('full_text_url') or data.get('pdf_url')
            if pdf_url:
                # Add .pdf extension if missing
                if not pdf_url.lower().endswith('.pdf'):
                    pdf_url = f"{pdf_url}.pdf"
                logging.debug(f"PubMed PDF URL for DOI {doi}: {pdf_url}")
                return pdf_url
            else:
                logging.debug(f"No PDF URL found in PubMed response for DOI {doi}")
        else:
            logging.warning(f"PubMed response for DOI {doi} returned status code {response.status_code}")
    except Exception as e:
        logging.error(f"PubMed error for {doi}: {str(e)}")
    return None

def download_pdf(url, save_path):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Accept': 'application/pdf,application/octet-stream,*/*'
    }
    logging.info(f"Starting download from URL: {url}")
    try:
        response = requests.get(url, stream=True, timeout=30, headers=headers)
        logging.debug(f"Download response for URL {url}: {response.status_code}")
        logging.debug(f"Download response content for URL {url}: {response.content}")
        
        if response.status_code == 200:
            content_type = response.headers.get('content-type', '').lower()
            logging.debug(f"Content-Type for URL {url}: {content_type}")
            # More permissive content type checking
            allowed_types = ['pdf', 'octet-stream', 'binary', 'download']
            if not any(t in content_type for t in allowed_types):
                # Try downloading anyway if content-type is missing or unknown
                if len(response.content) > 1000:  # At least 1KB
                    with open(save_path, 'wb') as f:
                        f.write(response.content)
                    return True, "Successfully downloaded (unknown content type)"
                return False, f"Not a PDF file (content-type: {content_type})"
            
            with open(save_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
            logging.info(f"Downloaded PDF to {save_path}")
            
            # Verify file size and basic PDF signature
            file_size = os.path.getsize(save_path)
            logging.debug(f"Downloaded file size for {save_path}: {file_size} bytes")
            if file_size < 1000:  # Less than 1KB is probably not a valid PDF
                os.remove(save_path)
                logging.warning(f"Downloaded file {save_path} is too small and has been removed.")
                return False, "Downloaded file too small"
            
            # Check for PDF signature
            try:
                with open(save_path, 'rb') as f:
                    header = f.read(4)
                    if header.startswith(b'%PDF'):
                        return True, "Successfully downloaded"
                    os.remove(save_path)
                    return False, "Not a valid PDF file"
            except:
                return True, "Successfully downloaded (signature check failed)"
        logging.warning(f"Failed to download URL {url} with status code {response.status_code}")
        return False, f"HTTP error: {response.status_code}"
    except Exception as e:
        if os.path.exists(save_path):
            os.remove(save_path)
            logging.error(f"Exception occurred. Removed incomplete file {save_path}. Error: {str(e)}")
        else:
            logging.error(f"Download error for URL {url}: {str(e)}")
        return False, f"Download error: {str(e)}"

def main():
    logging.info("Program started.")
    print("Loading reference file...")
    df, references, pdf_links = read_references('reference_list50.xlsx')
    os.makedirs('pdfs', exist_ok=True)
    logging.info("Ensuring 'pdfs' directory exists")
    
    print(f"\nAnalysis:")
    print(f"Total references in file: {len(df)}")
    print(f"Valid DOIs found: {len(references)}")
    logging.info(f"Total references in file: {len(df)}")
    logging.info(f"Valid DOIs found: {len(references)}")
    
    # Add a known valid DOI for testing
    test_doi = "10.1038/s41586-020-2649-2"  # Example
    if test_doi not in references:
        references.append(test_doi)
        pdf_links.append(None)  # Assume no PDF link for the test DOI
        logging.debug(f"Added test DOI: {test_doi}")

    logging.info(f"Processing {len(references)} DOIs")
    
    print("\nStarting download process...")
    logging.info("Starting download process.")
    
    results = []

    for doi, pdf_link in tqdm(zip(references, pdf_links), desc="Processing DOIs"):
        logging.debug(f"Starting process for DOI: {doi}")
        result = {
            'DOI': doi,
            'PDF_Found': False,
            'Source': None,
            'Download_Status': 'Not attempted',
            'File_Path': None,
            'Error_Message': None
        }
        
        # Try different sources, starting with Unpaywall and PubMed
        pdf_url = None
        for source, search_func in [
            ('Unpaywall', search_unpaywall),
            ('PubMed', search_pubmed)
        ]:
            logging.debug(f"Attempting to find PDF for DOI {doi} using {source}")
            pdf_url = search_func(doi)
            if pdf_url:
                result['Source'] = source
                result['PDF_Found'] = True
                logging.info(f"Found PDF URL for DOI {doi} from {source}: {pdf_url}")
                break
            else:
                logging.debug(f"No PDF URL found for DOI {doi} from {source}.")
        
        # If no PDF URL found from APIs, try Excel link as fallback
        if not pdf_url and pdf_link:
            logging.info(f"Using Excel PDF link for DOI {doi}: {pdf_link}")
            pdf_url = pdf_link
            result['Source'] = 'Excel Link'
            result['PDF_Found'] = True
        
        if pdf_url:
            filename = f"pdfs/{doi.replace('/', '_')}.pdf"
            success, message = download_pdf(pdf_url, filename)
            result['Download_Status'] = 'Success' if success else 'Failed'
            result['File_Path'] = filename if success else None
            result['Error_Message'] = None if success else message
            if success:
                logging.info(f"Successfully downloaded PDF for DOI {doi} to {filename}")
            else:
                logging.warning(f"Failed to download PDF for DOI {doi}: {message}")
        else:
            logging.debug(f"No URL obtained, skipping download for {doi}.")
            result['Download_Status'] = 'Failed'
            result['Error_Message'] = 'No PDF URL found'
            logging.warning(f"No PDF found for DOI {doi}")
        
        results.append(result)

    # Create results DataFrame and save to CSV
    results_df = pd.DataFrame(results)
    try:
        results_df.to_csv('results.csv', index=False)
        logging.info("Results saved to results.csv")
    except Exception as e:
        logging.error(f"Failed to save results.csv: {str(e)}")
    
    # Print summary
    successful = results_df['Download_Status'] == 'Success'
    print(f"\nSummary:")
    print(f"Total DOIs processed: {len(results_df)}")
    print(f"Successfully downloaded: {sum(successful)}")
    print(f"Failed: {sum(~successful)}")
    print("\nDetailed results saved to results.csv")
    logging.info("Program finished.")
    logging.info(f"Total DOIs processed: {len(results_df)}")
    logging.info(f"Successfully downloaded: {sum(successful)}")
    logging.info(f"Failed: {sum(~successful)}")

if __name__ == "__main__":
    main()