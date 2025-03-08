from ctadata.direct.api import APIClient
import logging
import os

logger = logging.getLogger(__name__)

# make sure profile dir exists
os.makedirs(os.path.dirname(APIClient.profile_dir), exist_ok=True)

for function in APIClient.__export_functions__:

    def decorate(function):
        def f(*args, **kwargs):
            api_client = APIClient(
                dev_instance=kwargs.pop('dev_instance', False))

            logger.info(
                'calling api_client=%s with function=%s args=%s, kwargs=%s',
                api_client, function, args, kwargs)

            for class_args in APIClient.__class_args__:
                if (class_arg_value := kwargs.pop(class_args, None)) \
                        is not None:
                    setattr(api_client, class_args, class_arg_value)

            return getattr(api_client, function)(*args, **kwargs)

        return f

    globals()[function] = decorate(function)
    logger.info('setting global function=%s to %s',
                function, globals()[function])
