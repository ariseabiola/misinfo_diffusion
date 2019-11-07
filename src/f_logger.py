import logging


log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
filename = 'logs.log'
logging.basicConfig(filename=filename, filemode='a', level=logging.DEBUG,
                    format=log_fmt)

# define a Handler which writes INFO messages or higher to the sys.stderr
console = logging.StreamHandler()
console.setLevel(logging.INFO)
# set a format which is simpler for console use
formatter = logging.Formatter('%(name)-12s: %(levelname)-8s - %(message)s')
# tell the handler to use this format
console.setFormatter(formatter)
# add the handler to the root logger
logging.getLogger('').addHandler(console)

logger = logging.getLogger('misinfo_diffusion')
