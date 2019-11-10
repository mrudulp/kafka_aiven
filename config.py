import configparser

class Config:

    def __init__(self, *args, **kwargs):
        """
        Initialises Configuration

        Parameters
        ----------
        args: Arguments to init. Here INI file name is expected as the only argument

        """
        self._config = configparser.ConfigParser()
        self._config.read(args[0])

    def get_broker_setting(self, settingKey):
        """
        Returns Settings related to Broker Section

        Parameters
        ----------
        settingKey: str
            -- Name of the Key whose corresponding value needs to be retrived

        Returns
        -------
        str
            -- Returns string value corresponding to the key
        """
        return self._config["broker"][f"{settingKey}"]

    def get_messages_setting(self, settingKey):
        """
        Returns Settings under Messages Section

        Parameters
        ----------
        settingKey: str
            -- Name of the Key whose corresponding value needs to be retrived

        Returns
        -------
        str
            -- Returns string value corresponding to the key
        """
        return self._config["messages"][f"{settingKey}"]