__version__ = ""

import logging

logger = logging.getLogger(__name__)

from .api import APIClient

for function in APIClient.__export_functions__:

    def decorate(function):
        def f(*args, **kwargs):
            api_client = APIClient()

            logger.info('calling api_client=%s with function=%s args=%s, kwargs=%s', api_client, function, args, kwargs)

            for class_arg in APIClient.__class_args__:
                if (class_arg_value := kwargs.pop(class_arg, None)) is not None:
                    setattr(api_client, class_arg, class_arg_value)
        
            return getattr(api_client, function)(*args, **kwargs)
        
        return f
            
        
    globals()[function] = decorate(function)
    logger.info('setting global function=%s to %s', function, globals()[function])
    