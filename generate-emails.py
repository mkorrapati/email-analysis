from tqdm import tqdm
import pandas as pd
import talon
from talon import quotations
from typing import Dict, List

tqdm.pandas(desc='Processing emails')
talon.init()

clause_list = [
    "Payment", 
    "Intellectual Property", 
    "Economic Surcharge", 
    "Warranty", 
    "Acceptance", 
    "Limitation of Liability", 
    "Assignment", 
    "Termination Rights", 
    "Change of Control", 
    "Governing Law & Dispute Resolution"
]


def get_first_email_in_thread(emails_df: pd.DataFrame):
    # Take first email from each thread
    first_emails_df = emails_df[emails_df.groupby('thread_id').timestamp.transform('min') == emails_df['timestamp']]
    return first_emails_df.sort_values(by=["thread_id", "timestamp"])

def get_current_body(full_body):
    def remove_identifier(line, marker=">"):
        if line.startswith(marker):
            return line[len(marker):]
        return line

    # Remove '>' at beginning of reply quote
    body_lines = full_body.split("\n")
    body_lines = [remove_identifier(line) for line in body_lines]

    full_body = "\n".join(body_lines)
    current_body = quotations.extract_from(full_body, 'text/plain')
    current_body = quotations.extract_from_plain(full_body)
    return current_body

def search_email_chain_by_keyword(dataframe: pd.DataFrame, keyword: str) -> List[Dict]:
    """
    Searches for a keyword in the email body and returns a chronological list of
    people who used the keyword throughout an email chain.

    Args:
    dataframe (pd.DataFrame): The dataframe containing the email data.
    keyword (str): The keyword to search for.

    Returns:
    List[Dict]: A chronological list of people who used the keyword in the email chain.
    """
    # Filter the dataframe for rows containing the keyword in the Body
    keyword_filtered = dataframe[dataframe['current_body'].str.contains(keyword, case=False, na=False)]

    # Sort the result by Date
    keyword_filtered_sorted = keyword_filtered.sort_values(by='timestamp')

    return keyword_filtered_sorted

def generate_email_threads(file_path: str, clause_index=1):
    # Load the CSV file (only 1000 rows for now)
    emails_threads_df = pd.read_csv(file_path)

    #Remove rows with no subject or 'Test' subject
    emails_threads_df = emails_threads_df[emails_threads_df['subject'] != '']
    emails_threads_df = emails_threads_df[emails_threads_df['subject'] != 'test']
    print(f"Number of emails loaded from file: {len(emails_threads_df)}")
    print(f"Columns: {emails_threads_df.columns}")

    # reame body to full_body
    emails_threads_df = emails_threads_df.rename({"body": "full_body"}, axis=1)

    # Generate current body
    emails_threads_df["current_body"] = emails_threads_df["full_body"].progress_apply(get_current_body)
    print(f"Number of emails after adding current body: {len(emails_threads_df)}")
    print(f"Columns: {emails_threads_df.columns}")

    #Remove rows with no subject or 'Test' subject
    emails_threads_df = emails_threads_df[emails_threads_df['current_body'] != '']
    print(f"Number of emails after removing emails with empty current body: {len(emails_threads_df)}")
    
    # Sanity check
    current_clause = clause_list[clause_index]
    print(f"Searching for clause: {current_clause}")
    search_results_chain = search_email_chain_by_keyword(emails_threads_df, current_clause)
    print(f"Number of emails with clause '{current_clause}': {len(search_results_chain)}")
    
    first_emails_with_clause_df = get_first_email_in_thread(search_results_chain)
    print(f"Number of first emails with clause '{current_clause}': {len(first_emails_with_clause_df)}")
    
    return emails_threads_df


if __name__ == '__main__':
    emails_file_path = "email_threads\CSV\email_thread_details.csv"
    df_pickled_file_path = "outputs\email_threads_cleaned.pkl"

    emails_df = generate_email_threads(emails_file_path)

    # Write cleaned df
    emails_df.to_pickle(df_pickled_file_path)
