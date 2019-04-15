import yaml
import os
from loguru import logger
from dotenv import find_dotenv, load_dotenv

class Settings:
    """Settings singleton for running jobs and processes
    
    Returns:
        Class -- Settings on singleton borg style
    """

    __shared_settings = {}
    def __init__(self):
        """Inits Settings 
        """

        self.__dict__ = self.__shared_settings
        logger.debug("Settings init - Reading Envs")
        load_dotenv(find_dotenv(), verbose=True)

        self.CONSUMER_KEY = os.getenv("CONSUMER_KEY")
        self.CONSUMER_SECRET= os.getenv("CONSUMER_SECRET")
        self.ACCESS_TOKEN_KEY= os.getenv("ACCESS_TOKEN_KEY")
        self.ACCESS_TOKEN_SECRET= os.getenv("ACCESS_TOKEN_SECRET")

        self.BOTOMETER_KEY = os.getenv("BOTOMETER_KEY")
        self.ELASTICSEARCH_URL = os.getenv("ELASTICSEARCH_URL")
        self.ELASTICSEARCH_STATUS_INDEX = os.getenv("ELASTICSEARCH_STATUS_INDEX")
        self.ELASTICSEARCH_USERS_INDEX = os.getenv("ELASTICSEARCH_USERS_INDEX")
        self.ELASTICSEARCH_BOT_INDEX = os.getenv("ELASTICSEARCH_BOT_INDEX")
        self.STATUS_JSON_BACKUP = os.getenv("STATUS_JSON_BACKUP")
        self.USER_JSON_BACKUP = os.getenv("USER_JSON_BACKUP")
        self.BOTOMETER_JSON_BACKUP = os.getenv("BOTOMETER_JSON_BACKUP")
        self.APERTIUM_URL = os.getenv("APERTIUM_URL")
        logger.debug("Settings Finished - Reading Envs")

    def _load_config(self, config_file):
        """Reads YML with config info into the singleton
        
        Arguments:
            config_file {str} -- File path with yml config settings
        
        Returns:
            [Object] -- Settings singleton itself
        """

        with open(config_file, 'r') as stream:
            try:
                logger.info("Loaded configs")
                config_settings = yaml.safe_load(stream)
                self.__dict__ = { **self.__dict__, **config_settings}
                logger.info("Added configs")
            except yaml.YAMLError as exc:
                print(exc)
        return self
