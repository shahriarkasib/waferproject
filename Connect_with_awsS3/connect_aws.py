import boto3
import smtplib
from email.mime.application import MIMEApplication
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import formatdate
import pandas as pd

class aws:

    def __init__(self):

        self.region_name='us-east-1'
        self.aws_access_key_id = 'HZYEL7PLOVRNNQ3VAIKA'[::-1]
        self.aws_secret_access_key = 'ol71iQq36rg6LuU8A5qefHPsTEPHzcDBNErs3+LM'[::-1]
        try:
            self.client = boto3.client(
                                    's3',
                                    aws_access_key_id=self.aws_access_key_id,
                                    aws_secret_access_key= self.aws_secret_access_key,
                                    region_name= self.region_name
                                )

            self.resource =  boto3.resource(
                    's3',
                    aws_access_key_id=self.aws_access_key_id,
                    aws_secret_access_key=self.aws_secret_access_key,
                    region_name=self.region_name
                )
        except Exception as e:
            print(e)

    def delete_modelfiles(self, bucket_name):
        bucket = self.resource.Bucket(bucket_name)
        bucket.objects.all().delete()


    def send_mail(self, send_from, send_to, subject, text, bucket_name):
        # assert isinstance(send_to, list)

        msg = MIMEMultipart()
        msg['From'] = send_from
        msg['Date'] = formatdate(localtime=True)
        msg['Subject'] = subject

        msg.attach(MIMEText(text))

        bucket_name = bucket_name

        bucket = self.resource.Bucket(bucket_name)
        files = [obj.key for obj in bucket.objects.filter()]

        for file in files:
            obj = self.client.get_object(
                Bucket=bucket_name,
                Key=file)
            csv = pd.read_csv(obj['Body'])

            part = MIMEBase('text', "csv")
            part.set_payload(csv.to_csv())
            part.add_header('Content-Disposition', 'attachment; filename="%s"' % file)
            msg.attach(part)

        smtp = smtplib.SMTP('smtp.gmail.com', 587)
        smtp.starttls()
        smtp.login('shahriarsourav@iut-dhaka.edu', '160021062Ss')
        smtp.sendmail(send_from, send_to, msg.as_string())
        smtp.close()


