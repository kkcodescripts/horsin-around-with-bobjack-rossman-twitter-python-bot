import os
import random
import tweepy
import csv
import subprocess
import base64
import logging
import time
import hashlib
from pathlib import Path
from typing import cast, List
from helpers.dynamo_db import DynamoHelper
from helpers.s3 import S3Helper
from datetime import datetime, timedelta

ROOT = Path(__file__).resolve().parents[0]
logger = logging.getLogger()
logger.setLevel(logging.INFO)


class BobJackRossManApp:
    """docstring for BobJackRossman"""
    def __init__(self):
        auth = tweepy.OAuthHandler(os.getenv("CONSUMER_KEY"), os.getenv("CONSUMER_SECRET"))
        auth.set_access_token(os.getenv("ACCESS_TOKEN"), os.getenv("ACCESS_TOKEN_SECRET"))
        self.twitter_api = tweepy.API(auth)
        self.tweets_text_file = 'quotes.csv'
        self.text_output_location = f'/tmp/{self.tweets_text_file}'
        self.font='/var/task/fonts/your_font.ttf'
        self.s3_bucket = 'your-s3-bucket'
        self.s3_dir = 'images_folder/'
        self.s3 = S3Helper()
        self.quotes_table = 'quotes_table_name'
        self.quotes_key = 'quote_partition_key_name'
        self.screenshots_table = 'screenshot_table_name'
        self.screenshots_key = 'screenshot_partition_key_name'
        self.combination_table = 'combined_table_name'
        self.combination_key = 'unique_md5_hash_of_final_image'

    @staticmethod
    def _encode_text(text):
        """base64 encoder for quotes and screenshots"""
        message_bytes = text.encode('ascii')
        base64_bytes = base64.b64encode(message_bytes)
        base64_text = base64_bytes.decode('ascii')
        return base64_text

    @staticmethod
    def _decode_text(base64_message):
        """base64 decoder for quotes and screenshots"""
        base64_bytes = base64_message.encode('ascii')
        message_bytes = base64.b64decode(base64_bytes)
        message = message_bytes.decode('ascii')
        return message

    @staticmethod
    def _generate_md5hash(image_name):
        """md5 hash generator for combined image"""
        with open(image_name, "rb") as f:
            file_hash = hashlib.md5()
            while chunk := f.read(8192):
                file_hash.update(chunk)
        return file_hash.hexdigest() 

    @staticmethod
    def _current_utc_rfc3339(days_ago=None):
        """datestamp for updated_on values"""
        utc_time = datetime.utcnow()
        if days_ago:
            utc_time = utc_time - timedelta(days=days_ago)
        return f'{utc_time.isoformat()}+00:00'

    def _get_quotes(self,possible_texts,count=1):
        """Get three quotes and find the least used one"""
        check_quotes_in_db_list = []
        for i in range(count):
            selected_tweet_text = random.choice(possible_texts)
            partition_value=self._encode_text(selected_tweet_text)
            check_quotes_in_db_list.append(DynamoHelper(table_name=self.quotes_table).get_item(table_name=self.quotes_table,partition_key=self.quotes_key,partition_value=partition_value))
            time.sleep(15)
        never_used_quote = next((obj for obj in check_quotes_in_db_list if obj['times_used']==0),"no match")
        if never_used_quote != 'no match':
            selected_tweet_text = str(never_used_quote[self.quotes_key])
            selected_tweet_text = self._decode_text(selected_tweet_text)
            self.quotes_value = selected_tweet_text
            return selected_tweet_text 
        else:
            db_item_counts = []
            for n in check_quotes_in_db_list:
                db_item_counts.append(int(n["times_used"]))
            index_of_items_equal_to_min = [i for i,e in enumerate(db_item_counts) if e == min(db_item_counts)] # get indices of min items
            chosen_index = random.choice(index_of_items_equal_to_min) # random choice of viable options
            final_choice = (check_quotes_in_db_list[chosen_index])
            #need to decode
            selected_tweet_text = str(final_choice[self.quotes_key])
            selected_tweet_text = self._decode_text(selected_tweet_text)
            self.quotes_value = selected_tweet_text
            return selected_tweet_text

    def _get_screenshots(self,available_images,count=1):
        """Get three screenshots and find the least used one"""
        check_screenshots_in_db_list = []
        for i in range(count):
            selected_tweet_image = random.choice(available_images) 
            partition_value=self._encode_text(selected_tweet_image)
            check_screenshots_in_db_list.append(DynamoHelper(table_name=self.screenshots_table).get_item(table_name=self.screenshots_table,partition_key=self.screenshots_key,partition_value=partition_value))
        never_used_screenshot = next((obj for obj in check_screenshots_in_db_list if obj['times_used']==0),"no match")
        if never_used_screenshot != 'no match':
            selected_tweet_image = never_used_screenshot[self.screenshots_key]
            selected_tweet_image = self._decode_text(selected_tweet_image)
            self.screenshots_value = selected_tweet_image
            return selected_tweet_image
        else:
            db_item_counts = []
            for n in check_screenshots_in_db_list:
                db_item_counts.append(int(n["times_used"]))
            index_of_items_equal_to_min = [i for i,e in enumerate(db_item_counts) if e == min(db_item_counts)] # get indices of min items
            chosen_index = random.choice(index_of_items_equal_to_min) # random choice of viable options
            final_choice = (check_screenshots_in_db_list[chosen_index])
            selected_tweet_image = str(final_choice[self.screenshots_key])
            selected_tweet_image = self._decode_text(selected_tweet_image)
            self.screenshots_value = selected_tweet_image
            return selected_tweet_image

    def get_tweet_text(self):
        """Access S3 buckand and get tweet to post from CSV file"""
        self.s3.download(self.s3_bucket, self.tweets_text_file, self.text_output_location)
        with open(self.text_output_location) as csvfile:
            reader = csv.DictReader(csvfile)
            possible_texts = [row["tweet"] for row in reader]
        selected_tweet_text = self._get_quotes(possible_texts,count=3) 
        return selected_tweet_text

    def get_tweet_image(self):
        """Access S3 bucket and pull image list. Select one."""
        available_images = self.s3.s3_list(self.s3_bucket, self.s3_dir)
        image_output_location = '/tmp/outputimg.png'
        selected_tweet_image = self._get_screenshots(available_images,count=3)
        self.s3.download(self.s3_bucket, selected_tweet_image, image_output_location)
        return image_output_location

    def create_tweet(self, tweet_text, tweet_image): 
        """Run Imagemagick to caption image. Additionally, check if image is unique."""
        cmd = 'convert'
        caption = f'caption:{tweet_text}'
        combined_tweet_location = '/tmp/combined.png'
        args = [cmd,'-background','#0008','-fill','white','-font',self.font,'-pointsize','43','-gravity','center','-size','900x', caption, tweet_image,'+swap','-gravity','south','-geometry','+0+25','-composite',combined_tweet_location]
        subprocess.run(args)
        self.combination_value=self._generate_md5hash(combined_tweet_location)
        check_for_unique_hash=DynamoHelper(table_name=self.combination_table).get_item(table_name=self.combination_table,partition_key=self.combination_key,partition_value=self.combination_value)
        if check_for_unique_hash['times_used'] == 0:
            self.is_combination_unqiue = True
        else:
            self.is_combination_unqiue = False
        return combined_tweet_location


    def update_db(self,target_table,partition_key,partition_value):
        """Database updater function"""
        current_time=self._current_utc_rfc3339()
        if partition_key == self.combination_key:
            partition_value=partition_value
        else:
            partition_value=self._encode_text(partition_value)
        response = DynamoHelper(table_name=target_table).get_item(table_name=target_table,partition_key=partition_key,partition_value=partition_value)
        db_times_used = 1
        if response['times_used'] == 0:
            DynamoHelper(table_name=target_table).put_item(table_name=target_table,partition_key=partition_key,partition_value=partition_value,times_used=db_times_used,updated_on=current_time)
        else:
            db_times_used = response['times_used'] + 1
            DynamoHelper(table_name=target_table).update_item(table_name=target_table,partition_key=partition_key,partition_value=partition_value,times_used=db_times_used,updated_on=current_time)  

    def post_tweet(self, tweet): 
        upload_media = self.twitter_api.media_upload(tweet)
        self.twitter_api.update_status(status='', media_ids=[upload_media.media_id])


def lambda_handler(event, context):
    logger.info("Authenticating with Twitter")
    logger.info("Setting up AWS env")
    bjr = BobJackRossManApp()
    logger.info("Generate tweet from S3 sources")
    while True:
        tweet_text = bjr.get_tweet_text()
        tweet_image = bjr.get_tweet_image() 
        tweet = bjr.create_tweet(tweet_text,tweet_image) 
        if bjr.is_combination_unqiue == True:
            logger.info("Update databases")
            bjr.update_db(target_table=bjr.quotes_table,partition_key=bjr.quotes_key,partition_value=bjr.quotes_value)
            bjr.update_db(target_table=bjr.screenshots_table,partition_key=bjr.screenshots_key,partition_value=bjr.screenshots_value)
            bjr.update_db(target_table=bjr.combination_table,partition_key=bjr.combination_key,partition_value=bjr.combination_value)
            logger.info(f"Upload and post tweet: {tweet}")
            response = bjr.post_tweet(tweet)
            
            return {"statusCode": 200, "tweetText": tweet, "response": response}
