import os,configparser
cwd = os.getcwd()
config = configparser.RawConfigParser()
config.read(f"{cwd}/config.cfg")

def clean_dict(my_dict):
    for key,value in my_dict.items():
        if key is not None:
            my_dict[key] = str(value)[1:-1]
    return my_dict

RDS = dict(config.items('RDS'))
REDSHIFT = dict(config.items('REDSHIFT'))
AWS = dict(config.items('AWS'))

RDS_config = clean_dict(my_dict=RDS)
REDSHIFT_config = clean_dict(my_dict=REDSHIFT)
AWS_config = clean_dict(my_dict=AWS)
