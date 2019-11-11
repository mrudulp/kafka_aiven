import configparser

class Config:

    def __init__(self, *args, **kwargs):
        """
        Initialises Configuration

        Parameters
        ----------
        args: Arguments to init. Here INI file name is expected as the only argument

        """
        config = configparser.ConfigParser()
        config.read(args[0])
        self._config = config

    def get_setting(self, section, settingKey):
        """
        Returns Settings under Given Section

        Parameters
        ----------
        section: str
            -- Name of the section under which setting needs to be retrived
        settingKey: str
            -- Name of the Key whose corresponding value needs to be retrived

        Returns
        -------
        str
            -- Returns string value corresponding to the key
        """
        return self._config[f"{section}"][f"{settingKey}"]
