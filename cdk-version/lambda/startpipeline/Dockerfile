# Get Public Image
FROM public.ecr.aws/lambda/python:3.10-x86_64

# Upgrade pip
RUN pip install --upgrade pip

COPY requirements.txt ${LAMBDA_TASK_ROOT}/
RUN pip install -r requirements.txt --target "${LAMBDA_TASK_ROOT}"

# Copy function code
COPY app/*.py ${LAMBDA_TASK_ROOT}/


# Set the CMD to your handler (could also be done as a parameter override outside of the Dockerfile)
CMD [ "start_execution.lambda_handler" ]