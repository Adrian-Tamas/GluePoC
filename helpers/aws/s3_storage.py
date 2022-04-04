import logging
import os
import polling2

from assertpy import assertpy
from botocore.exceptions import ClientError

from configuration import CONFIG

logger = logging.getLogger('glue-poc')


class S3Storage:

    def __init__(self, storage_name: str):
        self.s3 = CONFIG.aws_client.resource('s3')
        self.storage_name = storage_name
        self.storage = self.s3.Bucket(storage_name)
        self.client = CONFIG.aws_client.client('s3')

    def is_file_in_storage(self, file_key: str) -> bool:
        """
        Check if a partial file key can be found in the storage
        :param file_key: the partial file key
        :return: True if the key is in the storage, False otherwise
        """
        for item in self.storage.objects.all():
            if file_key in item.key:
                return True
        return False

    def remove_files_from_storage(self, file_key: str) -> None:
        """
        Delete a file from the storage
        :param file_key: the file key
        :return: None
        """
        file = self.s3.Object(bucket_name=self.storage_name, key=file_key)
        delete_result = file.delete()
        logger.debug(f"Delete request for file {file_key} had a return code "
                     f"of {delete_result['ResponseMetadata']['HTTPStatusCode']}")

    def remove_files_by_partial_key(self, partial_key: str) -> None:
        """
        Delete all files matching the partial key
        :param partial_key: the partial key to match against
        :return: None
        """
        for s3_object in self.storage.objects.all():
            if partial_key in s3_object.key:
                self.remove_files_from_storage(file_key=s3_object.key)

    def check_number_of_files_in_storage(self, partial_key=None) -> int:
        """
       Check the number of files found in storage but the files with a specific partial_key
       :param partial_key: the key for a specific search in the storage
       @key_list: the list of keys in the storage
       @nr_of_files_in_partial_key: number of files found for the specific partial_key in the storage
       @total_nr_files: number of files found in the storage
       :return: the value of nr_of_files_in_partial_key when a specific key is used as an argument when using the method
                the value of total_nr_files when no argument is given at use of this method
               """
        nr_of_files_in_partial_key = 0
        key_list = [item.key for item in self.storage.objects.all()]
        if partial_key is not None:
            for key in key_list:
                if partial_key in key:
                    nr_of_files_in_partial_key += 1
            return nr_of_files_in_partial_key
        else:
            return len(key_list)

    def check_that_all_files_are_present_in_storage(self,
                                                    common_file_key_prefix: str,
                                                    list_of_file_names: list) -> bool:
        """
        Check that all files in the provided list can be found in the storage
        :param common_file_key_prefix: the common part of the file key
        :param list_of_file_names: the list of files names to be checked
        :return: True if all the files in the list of file names are present in the storage
        """
        for file_name in list_of_file_names:
            file_key = f'{common_file_key_prefix}/{file_name}'
            logger.debug(f'the file KEY is {file_key}')
            if not self.is_file_in_storage(file_key=file_key):
                return False
        return True

    def wait_until_the_storage_contains_all_expected_files(self,
                                                           list_of_files: list,
                                                           common_file_key_prefix: str) -> None:
        """
        Wait until all the expected files are found in the storage
        :param list_of_files: list of expected files
        :param common_file_key_prefix:
        :return: None
        """
        try:
            polling2.poll(
                lambda:
                self.check_that_all_files_are_present_in_storage(common_file_key_prefix=common_file_key_prefix,
                                                                 list_of_file_names=list_of_files) is True,
                timeout=900,
                step=30)
        except polling2.TimeoutException:
            assertpy.fail(f"Unable to find all the files in the list: {list_of_files} in the storage. "
                          f"Files present in the storage: "
                          f"{self.list_all_files_matching_a_partial_key_found_in_the_storage(common_file_key_prefix)}")

    def list_all_files_matching_a_partial_key_found_in_the_storage(self, partial_file_key: str) -> list:
        """
        List all files matching a partial key that are in the storage
        :param partial_file_key: the partial file key used to filter
        :return: the list of matching file keys
        """
        list_of_file_keys = []
        for item in self.storage.objects.all():
            if partial_file_key in item.key:
                list_of_file_keys.append(item.key)
        return list_of_file_keys

    def get_last_modified_date_for_all_files_from_storage(self, partial_key: str) -> dict:
        """
        Get the last modified date of all the files in the storage matching the partial key
        :param partial_key: the partial key used to filter against
        :return: a dict with the file keys as the keys and the last modified timestamp as the values
        """
        last_modified_dates = {}
        for file in self.storage.objects.all():
            if partial_key in file.key:
                last_modified_dates.update({file.key: file.last_modified.timestamp()})
        return last_modified_dates

    def load_content_of_a_file_from_storage(self, file_key: str) -> str:
        """
        Read the content of an s3 file as a json
        :param file_key: the file key for a json file
        :return: the content of the file
        """
        content_object = self.s3.Object(bucket_name=self.storage_name, key=file_key)
        file_content = content_object.get()['Body'].read().decode('utf-8')
        return file_content

    def upload_file_to_storage(self, resource_name: str, object_name=None) -> bool:
        """Upload a file to an S3 bucket

        :param resource_name: File to upload
        :param object_name: S3 object name. If not specified then file_name is used
        :return: True if file was uploaded, else False
        """

        file_name = os.path.join(CONFIG.resource_path, resource_name)

        # If S3 object_name was not specified, use file_name
        if object_name is None:
            object_name = resource_name

        # Upload the file
        try:
            response = self.client.upload_file(file_name, self.storage_name, object_name)
        except ClientError as e:
            logging.error(e)
            return False
        return True

    def upload_file_with_metadata_to_storage(self, resource_name: str, object_name=None, metadata=None) -> bool:
        """Upload a file to an S3 bucket

        :param metadata: file metadata used to tag the file
        :param resource_name: File to upload
        :param object_name: S3 object name. If not specified then file_name is used
        :return: True if file was uploaded, else False
        """

        if metadata:
            extra_args = {'Metadata': metadata}
        else:
            extra_args = None
        file_name = os.path.join(CONFIG.resource_path, resource_name)

        # If S3 object_name was not specified, use file_name
        if object_name is None:
            object_name = resource_name

        # Upload the file
        try:
            response = self.client.upload_file(file_name, self.storage_name, object_name, ExtraArgs=extra_args)
        except ClientError as e:
            logging.error(e)
            return False
        return True

    def download_file_from_storage(self, file_key, file_location):
        self.client.download_file(
            self.storage_name,
            file_key,
            file_location,
        )