# Get Public Image
FROM public.ecr.aws/lambda/python:3.10-x86_64
RUN pip install --upgrade pip

# Copy function code
COPY app/*.py ${LAMBDA_TASK_ROOT}/
COPY app/utils ${LAMBDA_TASK_ROOT}/utils

# Set the CMD to your handler (could also be done as a parameter override outside of the Dockerfile)
CMD [ "textract-to-txt.lambda_handler" ]
