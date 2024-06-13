import psycopg2
import boto3
import os
import pandas as pd

# Database connection
conn = psycopg2.connect(
    dbname="medtank_db",
    user="postgres",
    password="jerry",
    host="localhost",
    port="5432"
)
cursor = conn.cursor()

# AWS S3 client The launching of S3 instance is yet to be done 
s3 = boto3.client('s3')

def upload_surgery_metadata(row):
    cursor.execute("""
        INSERT INTO surgeries (anon_id, gender, age, diagnosis, procedure_details, video_file_name, diagnosis_icd, revision_surgery, tumor_assessment, performing_physician_role)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING id
    """, (row['ANON ID'], row['Gender'], row['Age'], row['Diagnosis'], row['Procedure Details'], row['Video File name'], row['Diagnosis (ICD-11-WHO)'], row['Revision Surgery'], row['Tumor Assessment'], row['Procedure Performing Physician Role (SNOMED CT)']))
    surgery_id = cursor.fetchone()[0]
    conn.commit()
    return surgery_id

def upload_dicom_metadata(surgery_id, dicom_file_name, file_path, file_size):
    cursor.execute("""
        INSERT INTO dicom_files (surgery_id, dicom_file_name, file_path, file_size)
        VALUES (%s, %s, %s, %s)
    """, (surgery_id, dicom_file_name, file_path, file_size))
    conn.commit()

def upload_file_metadata(surgery_id, file_name, file_path, file_size):
    cursor.execute("""
        INSERT INTO files (surgery_id, file_name, file_path, file_size)
        VALUES (%s, %s, %s, %s)
    """, (surgery_id, file_name, file_path, file_size))
    conn.commit()

def upload_file_to_s3(file_path, bucket_name, s3_key):
    s3.upload_file(file_path, bucket_name, s3_key)

def extract_metadata_from_csv(csv_path):
    df = pd.read_csv(csv_path)
    df.rename(columns={'Diagnosis (ICD-11-WHO), see sheet "Diagnosis Appendix"':'Diagnosis (ICD-11-WHO)','Unnamed: 21': 'Dura Anfang','Unnamed: 22': 'Dura Ende','Unnamed: 23': 'Location', 'Unnamed: 24': 'Hospital', 'Unnamed: 25': 'Microscope'}, inplace=True)
    return df


def process_folder(folder_path, bucket_name, csv_path):
    df = extract_metadata_from_csv(csv_path)
    print(df.columns)
    for index, row in df.iterrows():
        surgery_id = upload_surgery_metadata(row)
    
        for root, dirs, files in os.walk(folder_path):
            for file in files:
                file_path = os.path.join(root, file)
                s3_key = f"{row['anon_id']}/{file}"
                # upload_file_to_s3(file_path, bucket_name, s3_key)
                
                file_size = os.path.getsize(file_path)
                if file.endswith('.dcm'):
                    upload_dicom_metadata(surgery_id, file, s3_key, file_size)
                else:
                    upload_file_metadata(surgery_id, file, s3_key, file_size)

def main():
    csv_path = "medtankdata.csv"
    folder_path = "/mnt/LenaOneDriveData"
    bucket_name = "medtank-storage"
    
    process_folder(folder_path, bucket_name, csv_path)

if __name__ == "__main__":
    main()
