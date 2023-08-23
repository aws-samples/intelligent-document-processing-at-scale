# Get Public Image
FROM public.ecr.aws/lambda/python:3.10-x86_64
RUN pip install --upgrade pip

# do this at the top so docker cache works
# Install packages
COPY requirements.txt ${LAMBDA_TASK_ROOT}
RUN  pip install -r requirements.txt --target "${LAMBDA_TASK_ROOT}"

# Copy function code
COPY app/*.py ${LAMBDA_TASK_ROOT}/
COPY app/utils ${LAMBDA_TASK_ROOT}/utils

# Set the CMD to your handler (could also be done as a parameter override outside of the Dockerfile)
CMD [ "textract-to-txt.lambda_handler" ]
