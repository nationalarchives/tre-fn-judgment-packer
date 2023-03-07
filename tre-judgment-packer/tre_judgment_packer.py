import logging
import boto3
import s3_lib.object_lib
from s3_lib import tar_lib
from botocore.exceptions import ClientError
from s3_lib import common_lib
from tre_event_lib import tre_event_api

# Set global logging options; AWS environment may override this though
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Env vars

OUT_BUCKET = common_lib.get_env_var(
    "TRE_S3_JUDGMENT_OUT_BUCKET", must_exist=True, must_have_value=True
)
URL_EXPIRY = common_lib.get_env_var(
    "TRE_PRESIGNED_URL_EXPIRY", must_exist=True, must_have_value=True
)
PROCESS = common_lib.get_env_var(
    "TRE_PROCESS_NAME", must_exist=True, must_have_value=True
)
PRODUCER = common_lib.get_env_var(
    "TRE_SYSTEM_NAME", must_exist=True, must_have_value=True
)
ENVIRONMENT = common_lib.get_env_var(
    "TRE_ENVIRONMENT", must_exist=True, must_have_value=True
)

# Message Vars

KEY_S3_FOLDER_URL = "s3-folder-url"
KEY_S3_SHA256_URL = "s3-sha256-url"

# Lambda vars

KEY_S3_OBJECT_ROOT = "s3FolderName"
SOURCE_BUCKET_NAME = "s3Bucket"
EVENT_NAME_INPUT = "tre-parsed-judgement"
EVENT_NAME_OUTPUT_OK = "packed-judgment"
EVENT_NAME_OUTPUT_ERROR = "packed-judgment-error"
PARSED_JUDGMENT_FILE_PATH = "parsed/judgment/"  # TODO check with Mark if this is going to the same place as currently?


def handler(event, context):
    """
    Given the presence of a parsed judgment, this lambda zips up the contents of all files and generates a
    one time link in s3 to be sent onto the caselaw team for consumption.
    """

    logger.info(f'handler start: event="{event}"')

    tre_event_api.validate_event(event=event, schema_name=EVENT_NAME_INPUT)
    s3_source_bucket = event[tre_event_api.KEY_PARAMETERS][EVENT_NAME_INPUT][
        tre_event_api.KEY_S3_BUCKET
    ]
    consignment_reference = event[tre_event_api.KEY_PARAMETERS][EVENT_NAME_INPUT][
        tre_event_api.KEY_REFERENCE
    ]

    try:

        logger.info(
            f"Packing consignment files {consignment_reference} from {s3_source_bucket} to {OUT_BUCKET}"
        )

        # Create a list of items to be zipped
        # TODO check with Mark that the key for his parsed objects is just the consignment reference

        files_to_zip = s3_lib.object_lib.s3_ls(
            bucket_name=SOURCE_BUCKET_NAME,
            object_filter=f"{PARSED_JUDGMENT_FILE_PATH}{consignment_reference}/",
        )

        logger.info(
            f"packing {len(files_to_zip)} files from consignment {consignment_reference}"
        )

        # Tar the objects up

        packed_judgment_file_name = f".tar.gz"

        s3_lib.tar_lib.s3_objects_to_s3_tar_gz_file(
            s3_bucket_in=s3_source_bucket,
            s3_object_names=files_to_zip,
            tar_gz_object=packed_judgment_file_name,
            s3_bucket_out=OUT_BUCKET,
        )

        # Generate pre-signed URL
        s3 = boto3.client("s3")

        try:
            s3_presigned_link = s3.generate_presigned_url(
                "get_object",
                Params={
                    "Bucket": OUT_BUCKET,
                    "Key": packed_judgment_file_name,
                },  # TODO check file-naming convention
                ExpiresIn=URL_EXPIRY,
            )
        except ClientError as e:
            logging.error(e)
            return None

        output_parameter_block = {
            EVENT_NAME_OUTPUT_OK: {
                tre_event_api.KEY_REFERENCE: consignment_reference,
                KEY_S3_FOLDER_URL: s3_presigned_link,
            }
        }

        event_output_success = tre_event_api.create_event(
            environment=ENVIRONMENT,
            producer=PRODUCER,
            process=PROCESS,
            event_name=EVENT_NAME_OUTPUT_OK,
            prior_event=event,
            parameters=output_parameter_block,
        )

        logger.info(f"event_output_success:\n%s\n", event_output_success)
        return event_output_success

    except ValueError as e:
        logging.error("handler error: %s", str(e))
        output_parameter_block = {
            EVENT_NAME_OUTPUT_ERROR: {
                tre_event_api.KEY_REFERENCE: consignment_reference,
                tre_event_api.KEY_ERRORS: [str(e)],
            }
        }

        event_output_error = tre_event_api.create_event(
            environment=ENVIRONMENT,
            producer=PRODUCER,
            process=PROCESS,
            event_name=EVENT_NAME_OUTPUT_ERROR,
            prior_event=event,
            parameters=output_parameter_block,
        )

        logger.info(f"event_output_error:\n%s\n", event_output_error)
        return event_output_error
