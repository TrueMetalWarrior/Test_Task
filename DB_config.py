from configparser import ConfigParser

# Функция для загрузки данных из ".ini" файла для подключения к БД
def load_config(filename='database.ini', section='postgreSQL'):
    parser = ConfigParser()
    parser.read(filename)
    db_connection_config = {}
    if parser.has_section(section):
        connect_settings = parser.items(section)
        for setting in connect_settings:
            db_connection_config[setting[0]] = setting[1]
        return db_connection_config
    else:
        raise Exception('Section {0} not found in the {1} file'.format(section, filename))



if __name__ == '__main__':
    config = load_config()
    print(config)


