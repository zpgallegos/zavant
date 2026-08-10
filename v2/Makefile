PYTHON ?= python3
AWS ?= aws
AWS_REGION ?= us-east-1
EXPECTED_AWS_ACCOUNT_ID ?=
VENV := .venv
VENV_PYTHON := $(VENV)/bin/python
INFRASTRUCTURE_TEMPLATE := infrastructure/template.yaml
STACK_NAME ?= zavant-acquisition-prod
ENVIRONMENT ?= prod
S3_PREFIX ?= lake
INITIAL_SCHEDULE_DATE ?=
INITIAL_CORRECTION_WATERMARK ?=
DAILY_SCHEDULE_EXPRESSION ?= cron(0 6 * * ? *)
DAILY_SCHEDULE_TIMEZONE ?= America/Los_Angeles
DAILY_SCHEDULE_STATE ?= ENABLED
LAMBDA_BUILD_DIR := build/lambda
LAMBDA_ARCHIVE := build/zavant-lambda.zip
DEPENDENCY_CONSTRAINTS := constraints.txt
LAMBDA_CODE_PREFIX := deployments/lambda
LAMBDA_EVENT_FILE ?= infrastructure/manual-event.json
LAMBDA_RESPONSE_FILE ?= build/lambda-response.json

.PHONY: bootstrap test check infra-check-account infra-check-bootstrap infra-validate infra-bootstrap lambda-package infra-deploy lambda-invoke help

help:
	@echo "bootstrap      create the local Python environment"
	@echo "test           run the unit test suite"
	@echo "check          run all local quality checks"
	@echo "infra-validate validate the CloudFormation template"
	@echo "infra-bootstrap  create the bucket and execution role on a new stack"
	@echo "lambda-package   build the Lambda zip with locked application dependencies"
	@echo "infra-deploy     package and deploy the scheduled production Lambda"
	@echo "lambda-invoke    manually invoke the deployed function"

bootstrap:
	@$(PYTHON) -c 'import sys; assert sys.version_info >= (3, 9), "Python 3.9+ is required"'
	@test -x $(VENV_PYTHON) || $(PYTHON) -m venv $(VENV)
	@$(VENV_PYTHON) -m pip install --constraint $(DEPENDENCY_CONSTRAINTS) --editable '.[dev]'
	@$(VENV_PYTHON) -c 'import sys; print(f"ready: {sys.executable} ({sys.version.split()[0]})")'

test:
	@PYTHONPATH=src $(VENV_PYTHON) -m unittest discover -s tests -v

check:
	@PYTHONPATH=src $(VENV_PYTHON) -m compileall -q src tests
	@$(VENV_PYTHON) -m ruff check src tests
	@PYTHONPATH=src $(VENV_PYTHON) -m pyright
	@PYTHONPATH=src $(VENV_PYTHON) -m coverage erase
	@PYTHONPATH=src $(VENV_PYTHON) -m coverage run -m unittest discover -s tests
	@$(VENV_PYTHON) -m coverage report
	@PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src $(VENV_PYTHON) -c 'import zavant; from zavant.lambda_handler import lambda_handler; assert callable(lambda_handler)'

infra-validate:
	@$(AWS) cloudformation validate-template \
		--region $(AWS_REGION) \
		--template-body file://$(abspath $(INFRASTRUCTURE_TEMPLATE))

infra-check-account:
	@if [ -z "$(EXPECTED_AWS_ACCOUNT_ID)" ]; then \
		echo "EXPECTED_AWS_ACCOUNT_ID is required" >&2; \
		exit 1; \
	fi
	@actual_account="$$( $(AWS) sts get-caller-identity --region $(AWS_REGION) --query Account --output text )"; \
	if [ "$$actual_account" != "$(EXPECTED_AWS_ACCOUNT_ID)" ]; then \
		echo "Refusing deployment: expected AWS account $(EXPECTED_AWS_ACCOUNT_ID), received $$actual_account" >&2; \
		exit 1; \
	fi

infra-check-bootstrap:
	@if [ -z "$(INITIAL_SCHEDULE_DATE)" ]; then \
		echo "INITIAL_SCHEDULE_DATE is required" >&2; \
		exit 1; \
	fi
	@if [ -z "$(INITIAL_CORRECTION_WATERMARK)" ]; then \
		echo "INITIAL_CORRECTION_WATERMARK is required" >&2; \
		exit 1; \
	fi

infra-bootstrap: infra-check-account
	@$(AWS) cloudformation deploy \
		--region $(AWS_REGION) \
		--template-file $(INFRASTRUCTURE_TEMPLATE) \
		--stack-name $(STACK_NAME) \
		--capabilities CAPABILITY_IAM \
		--parameter-overrides \
			EnvironmentName=$(ENVIRONMENT) \
			AcquisitionPrefix=$(S3_PREFIX) \
		--tags Project=zavant Component=acquisition Environment=$(ENVIRONMENT) ManagedBy=cloudformation

lambda-package:
	@rm -rf $(LAMBDA_BUILD_DIR) $(LAMBDA_ARCHIVE)
	@mkdir -p $(LAMBDA_BUILD_DIR)
	@$(VENV_PYTHON) -m pip install --quiet --no-compile \
		--constraint $(DEPENDENCY_CONSTRAINTS) \
		--target $(LAMBDA_BUILD_DIR) \
		.
	@(cd $(LAMBDA_BUILD_DIR) && zip -q -r -X $(abspath $(LAMBDA_ARCHIVE)) .)
	@PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=$(LAMBDA_BUILD_DIR) $(VENV_PYTHON) -S -c 'from zavant.lambda_handler import lambda_handler; assert callable(lambda_handler)'

infra-deploy: infra-check-account infra-check-bootstrap lambda-package
	@bucket="$$($(AWS) cloudformation describe-stacks \
		--region $(AWS_REGION) \
		--stack-name $(STACK_NAME) \
		--query 'Stacks[0].Outputs[?OutputKey==`AcquisitionBucketName`].OutputValue | [0]' \
		--output text)"; \
	digest="$$(shasum -a 256 $(LAMBDA_ARCHIVE) | cut -d ' ' -f 1)"; \
	code_key="$(LAMBDA_CODE_PREFIX)/$$digest.zip"; \
	$(AWS) s3 cp $(LAMBDA_ARCHIVE) "s3://$$bucket/$$code_key" \
		--region $(AWS_REGION) \
		--only-show-errors; \
	$(AWS) cloudformation deploy \
		--region $(AWS_REGION) \
		--template-file $(INFRASTRUCTURE_TEMPLATE) \
		--stack-name $(STACK_NAME) \
		--capabilities CAPABILITY_IAM \
		--parameter-overrides \
			EnvironmentName=$(ENVIRONMENT) \
			AcquisitionPrefix=$(S3_PREFIX) \
			InitialScheduleDate=$(INITIAL_SCHEDULE_DATE) \
			InitialCorrectionWatermark=$(INITIAL_CORRECTION_WATERMARK) \
			DailyScheduleExpression="$(DAILY_SCHEDULE_EXPRESSION)" \
			DailyScheduleTimezone=$(DAILY_SCHEDULE_TIMEZONE) \
			DailyScheduleState=$(DAILY_SCHEDULE_STATE) \
			LambdaCodeS3Key="$$code_key" \
		--tags Project=zavant Component=acquisition Environment=$(ENVIRONMENT) ManagedBy=cloudformation

lambda-invoke: infra-check-account
	@mkdir -p $(dir $(LAMBDA_RESPONSE_FILE))
	@$(AWS) lambda invoke \
		--region $(AWS_REGION) \
		--function-name zavant-acquisition-daily-$(ENVIRONMENT) \
		--cli-binary-format raw-in-base64-out \
		--payload fileb://$(LAMBDA_EVENT_FILE) \
		$(LAMBDA_RESPONSE_FILE)
