# Local environment configuration

DOTENV_FILE ?= .env
-include $(DOTENV_FILE)

export ZAVANT_ANALYTICAL_MAX_PROJECTION_PARTITIONS
export ZAVANT_AWS_ACCOUNT_ID
export ZAVANT_AWS_REGION
export ZAVANT_DAILY_SCHEDULE_EXPRESSION
export ZAVANT_DAILY_SCHEDULE_STATE
export ZAVANT_DAILY_SCHEDULE_TIMEZONE
export ZAVANT_DATA_DIR
export ZAVANT_DEPLOYMENT_ENVIRONMENT
export ZAVANT_INITIAL_CORRECTION_WATERMARK
export ZAVANT_INITIAL_SCHEDULE_DATE
export ZAVANT_MLB_API_BASE_URL
export ZAVANT_S3_BUCKET
export ZAVANT_S3_PREFIX

# Toolchain and common paths

PYTHON ?= python3
AWS_CLI ?= aws

BUILD_DIR := build
DBT_PROJECT_DIR := dbt
VENV_DIR := .venv
VENV_PYTHON := $(VENV_DIR)/bin/python
VENV_DBT := $(abspath $(VENV_DIR)/bin/dbt)
PYTHON_CONSTRAINTS_FILE := constraints.txt

# AWS deployment identity

configuration_or_default = $(if $(strip $(1)),$(strip $(1)),$(2))
strip_single_quotes = $(subst ',,$(1))

PROJECT_NAME := zavant
AWS_ACCOUNT_ID ?= $(ZAVANT_AWS_ACCOUNT_ID)
AWS_REGION ?= $(call configuration_or_default, \
	$(ZAVANT_AWS_REGION),us-east-1)
DEPLOYMENT_ENVIRONMENT ?= $(call configuration_or_default, \
	$(ZAVANT_DEPLOYMENT_ENVIRONMENT),prod)

# Acquisition workload

ACQUISITION_TEMPLATE := infrastructure/acquisition-stack.yaml
ACQUISITION_STACK_NAME ?= $(PROJECT_NAME)-acquisition-$(DEPLOYMENT_ENVIRONMENT)
ACQUISITION_S3_PREFIX ?= $(call configuration_or_default, \
	$(ZAVANT_S3_PREFIX),lake)
ACQUISITION_INITIAL_SCHEDULE_DATE ?= $(ZAVANT_INITIAL_SCHEDULE_DATE)
ACQUISITION_INITIAL_CORRECTION_WATERMARK ?= \
	$(ZAVANT_INITIAL_CORRECTION_WATERMARK)

ACQUISITION_LAMBDA_BUILD_DIR := $(BUILD_DIR)/lambda
ACQUISITION_LAMBDA_ARCHIVE := $(BUILD_DIR)/zavant-lambda.zip
ACQUISITION_LAMBDA_CODE_PREFIX := deployments/lambda
ACQUISITION_LAMBDA_EVENT_FILE ?= infrastructure/manual-event.json
ACQUISITION_LAMBDA_RESPONSE_FILE ?= $(BUILD_DIR)/lambda-response.json

# Analytical workload

ANALYTICAL_TEMPLATE := infrastructure/analytical-projection-stack.yaml
ANALYTICAL_STACK_NAME ?= $(PROJECT_NAME)-analytics-$(DEPLOYMENT_ENVIRONMENT)
ANALYTICAL_GLUE_LIBRARY_ARCHIVE := $(BUILD_DIR)/zavant-glue.zip
ANALYTICAL_GLUE_SCRIPT := jobs/project_analytical.py
ANALYTICAL_GLUE_CODE_PREFIX := deployments/glue
ANALYTICAL_MAX_PROJECTION_PARTITIONS ?= $(call configuration_or_default, \
	$(ZAVANT_ANALYTICAL_MAX_PROJECTION_PARTITIONS),64)

# Daily workflow

DAILY_WORKFLOW_TEMPLATE := infrastructure/daily-workflow-stack.yaml
DAILY_WORKFLOW_STACK_NAME ?= $(PROJECT_NAME)-daily-workflow-$(DEPLOYMENT_ENVIRONMENT)
DAILY_WORKFLOW_INPUT_FILE ?= infrastructure/manual-event.json
DAILY_WORKFLOW_SCHEDULE_EXPRESSION ?= \
	$(call strip_single_quotes,$(ZAVANT_DAILY_SCHEDULE_EXPRESSION))
DAILY_WORKFLOW_SCHEDULE_TIMEZONE ?= $(ZAVANT_DAILY_SCHEDULE_TIMEZONE)
DAILY_WORKFLOW_SCHEDULE_STATE ?= $(ZAVANT_DAILY_SCHEDULE_STATE)

.PHONY: \
	acquisition-check-bootstrap \
	acquisition-infra-bootstrap \
	acquisition-infra-deploy \
	acquisition-infra-validate \
	analytics-infra-deploy \
	analytics-infra-validate \
	aws-check-account \
	bootstrap \
	check \
	dbt-debug \
	glue-package \
	glue-start \
	help \
	lambda-invoke \
	lambda-package \
	test \
	workflow-check-schedule \
	workflow-infra-deploy \
	workflow-infra-validate \
	workflow-start

help:
	@echo "bootstrap                   create the local Python environment"
	@echo "test                        run the unit test suite"
	@echo "check                       run all local quality checks"
	@echo "dbt-debug                   validate the dev dbt/Athena connection"
	@echo "acquisition-infra-validate  validate the acquisition template"
	@echo "analytics-infra-validate    validate the Glue/Iceberg template"
	@echo "workflow-infra-validate     validate the Step Functions template"
	@echo "acquisition-infra-bootstrap create the acquisition bucket and role"
	@echo "lambda-package              build the Lambda deployment archive"
	@echo "glue-package                build the Glue Python library archive"
	@echo "acquisition-infra-deploy    deploy the acquisition Lambda"
	@echo "analytics-infra-deploy      deploy the Glue projection job"
	@echo "workflow-infra-deploy       deploy the daily workflow and schedule"
	@echo "lambda-invoke               manually invoke acquisition"
	@echo "glue-start                  manually start analytical projection"
	@echo "workflow-start              manually start the complete workflow"

bootstrap:
	@$(PYTHON) -c 'import sys; assert sys.version_info >= (3, 9), "Python 3.9+ is required"'
	@test -x $(VENV_PYTHON) || $(PYTHON) -m venv $(VENV_DIR)
	@$(VENV_PYTHON) -m pip install --constraint $(PYTHON_CONSTRAINTS_FILE) --editable '.[dev]'
	@$(VENV_PYTHON) -c 'import sys; print(f"ready: {sys.executable} ({sys.version.split()[0]})")'

test:
	@PYTHONPATH=src $(VENV_PYTHON) -m unittest discover -s tests -v

check:
	@PYTHONPATH=src $(VENV_PYTHON) -m compileall -q src tests jobs
	@$(VENV_PYTHON) -m ruff check src tests jobs
	@PYTHONPATH=src $(VENV_PYTHON) -m pyright
	@PYTHONPATH=src $(VENV_PYTHON) -m coverage erase
	@PYTHONPATH=src $(VENV_PYTHON) -m coverage run -m unittest discover -s tests
	@$(VENV_PYTHON) -m coverage report
	@PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src $(VENV_PYTHON) -c 'import zavant; from zavant.lambda_handler import lambda_handler; assert callable(lambda_handler)'

dbt-debug:
	@cd $(DBT_PROJECT_DIR) && $(VENV_DBT) debug --target dev

acquisition-infra-validate:
	@$(AWS_CLI) cloudformation validate-template \
		--region $(AWS_REGION) \
		--template-body file://$(abspath $(ACQUISITION_TEMPLATE))

analytics-infra-validate:
	@$(AWS_CLI) cloudformation validate-template \
		--region $(AWS_REGION) \
		--template-body file://$(abspath $(ANALYTICAL_TEMPLATE))

workflow-infra-validate:
	@$(AWS_CLI) cloudformation validate-template \
		--region $(AWS_REGION) \
		--template-body file://$(abspath $(DAILY_WORKFLOW_TEMPLATE))

aws-check-account:
	@if [ -z "$(AWS_ACCOUNT_ID)" ]; then \
		echo "ZAVANT_AWS_ACCOUNT_ID is required" >&2; \
		exit 1; \
	fi
	@actual_account="$$( $(AWS_CLI) sts get-caller-identity --region $(AWS_REGION) --query Account --output text )"; \
	if [ "$$actual_account" != "$(AWS_ACCOUNT_ID)" ]; then \
		echo "Refusing deployment: expected AWS account $(AWS_ACCOUNT_ID), received $$actual_account" >&2; \
		exit 1; \
	fi

acquisition-check-bootstrap:
	@if [ -z "$(ACQUISITION_INITIAL_SCHEDULE_DATE)" ]; then \
		echo "ZAVANT_INITIAL_SCHEDULE_DATE is required" >&2; \
		exit 1; \
	fi
	@if [ -z "$(ACQUISITION_INITIAL_CORRECTION_WATERMARK)" ]; then \
		echo "ZAVANT_INITIAL_CORRECTION_WATERMARK is required" >&2; \
		exit 1; \
	fi

workflow-check-schedule:
	@if [ -z "$(DAILY_WORKFLOW_SCHEDULE_EXPRESSION)" ]; then \
		echo "ZAVANT_DAILY_SCHEDULE_EXPRESSION is required" >&2; \
		exit 1; \
	fi
	@if [ -z "$(DAILY_WORKFLOW_SCHEDULE_TIMEZONE)" ]; then \
		echo "ZAVANT_DAILY_SCHEDULE_TIMEZONE is required" >&2; \
		exit 1; \
	fi
	@if [ -z "$(DAILY_WORKFLOW_SCHEDULE_STATE)" ]; then \
		echo "ZAVANT_DAILY_SCHEDULE_STATE is required" >&2; \
		exit 1; \
	fi

acquisition-infra-bootstrap: aws-check-account
	@$(AWS_CLI) cloudformation deploy \
		--region $(AWS_REGION) \
		--template-file $(ACQUISITION_TEMPLATE) \
		--stack-name $(ACQUISITION_STACK_NAME) \
		--capabilities CAPABILITY_IAM \
		--parameter-overrides \
			EnvironmentName=$(DEPLOYMENT_ENVIRONMENT) \
			AcquisitionPrefix=$(ACQUISITION_S3_PREFIX) \
		--tags Project=$(PROJECT_NAME) Component=acquisition Environment=$(DEPLOYMENT_ENVIRONMENT) ManagedBy=cloudformation

lambda-package:
	@rm -rf $(ACQUISITION_LAMBDA_BUILD_DIR) $(ACQUISITION_LAMBDA_ARCHIVE)
	@mkdir -p $(ACQUISITION_LAMBDA_BUILD_DIR)
	@$(VENV_PYTHON) -m pip install --quiet --no-compile \
		--constraint $(PYTHON_CONSTRAINTS_FILE) \
		--target $(ACQUISITION_LAMBDA_BUILD_DIR) \
		.
	@(cd $(ACQUISITION_LAMBDA_BUILD_DIR) && zip -q -r -X $(abspath $(ACQUISITION_LAMBDA_ARCHIVE)) .)
	@PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=$(ACQUISITION_LAMBDA_BUILD_DIR) $(VENV_PYTHON) -S -c 'from zavant.lambda_handler import lambda_handler; assert callable(lambda_handler)'

glue-package:
	@rm -f $(ANALYTICAL_GLUE_LIBRARY_ARCHIVE)
	@mkdir -p $(dir $(ANALYTICAL_GLUE_LIBRARY_ARCHIVE))
	@(cd src && zip -q -r -X $(abspath $(ANALYTICAL_GLUE_LIBRARY_ARCHIVE)) zavant -x '*/__pycache__/*' '*.pyc')
	@PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src $(VENV_PYTHON) -S -c 'from zavant.projection.glue_job import main; assert callable(main)'

acquisition-infra-deploy: \
	aws-check-account \
	acquisition-check-bootstrap \
	lambda-package
	@bucket="$$($(AWS_CLI) cloudformation describe-stacks \
		--region $(AWS_REGION) \
		--stack-name $(ACQUISITION_STACK_NAME) \
		--query 'Stacks[0].Outputs[?OutputKey==`AcquisitionBucketName`].OutputValue | [0]' \
		--output text)"; \
	digest="$$(shasum -a 256 $(ACQUISITION_LAMBDA_ARCHIVE) | cut -d ' ' -f 1)"; \
	code_key="$(ACQUISITION_LAMBDA_CODE_PREFIX)/$$digest.zip"; \
	$(AWS_CLI) s3 cp $(ACQUISITION_LAMBDA_ARCHIVE) "s3://$$bucket/$$code_key" \
		--region $(AWS_REGION) \
		--only-show-errors; \
	$(AWS_CLI) cloudformation deploy \
		--region $(AWS_REGION) \
		--template-file $(ACQUISITION_TEMPLATE) \
		--stack-name $(ACQUISITION_STACK_NAME) \
		--capabilities CAPABILITY_IAM \
		--parameter-overrides \
			EnvironmentName=$(DEPLOYMENT_ENVIRONMENT) \
			AcquisitionPrefix=$(ACQUISITION_S3_PREFIX) \
			InitialScheduleDate=$(ACQUISITION_INITIAL_SCHEDULE_DATE) \
			InitialCorrectionWatermark=$(ACQUISITION_INITIAL_CORRECTION_WATERMARK) \
			LambdaCodeS3Key="$$code_key" \
		--tags Project=$(PROJECT_NAME) Component=acquisition Environment=$(DEPLOYMENT_ENVIRONMENT) ManagedBy=cloudformation

analytics-infra-deploy: aws-check-account glue-package
	@bucket="$$($(AWS_CLI) cloudformation describe-stacks \
		--region $(AWS_REGION) \
		--stack-name $(ACQUISITION_STACK_NAME) \
		--query 'Stacks[0].Outputs[?OutputKey==`AcquisitionBucketName`].OutputValue | [0]' \
		--output text)"; \
	prefix="$$($(AWS_CLI) cloudformation describe-stacks \
		--region $(AWS_REGION) \
		--stack-name $(ACQUISITION_STACK_NAME) \
		--query 'Stacks[0].Outputs[?OutputKey==`AcquisitionPrefix`].OutputValue | [0]' \
		--output text)"; \
	library_digest="$$(shasum -a 256 $(ANALYTICAL_GLUE_LIBRARY_ARCHIVE) | cut -d ' ' -f 1)"; \
	script_digest="$$(shasum -a 256 $(ANALYTICAL_GLUE_SCRIPT) | cut -d ' ' -f 1)"; \
	library_key="$(ANALYTICAL_GLUE_CODE_PREFIX)/$$library_digest/zavant-glue.zip"; \
	script_key="$(ANALYTICAL_GLUE_CODE_PREFIX)/$$script_digest/project_analytical.py"; \
	$(AWS_CLI) s3 cp $(ANALYTICAL_GLUE_LIBRARY_ARCHIVE) "s3://$$bucket/$$library_key" \
		--region $(AWS_REGION) \
		--only-show-errors; \
	$(AWS_CLI) s3 cp $(ANALYTICAL_GLUE_SCRIPT) "s3://$$bucket/$$script_key" \
		--region $(AWS_REGION) \
		--only-show-errors; \
	$(AWS_CLI) cloudformation deploy \
		--region $(AWS_REGION) \
		--template-file $(ANALYTICAL_TEMPLATE) \
		--stack-name $(ANALYTICAL_STACK_NAME) \
		--capabilities CAPABILITY_IAM \
		--parameter-overrides \
			EnvironmentName=$(DEPLOYMENT_ENVIRONMENT) \
			DataBucketName="$$bucket" \
			DataPrefix="$$prefix" \
			GlueLibraryS3Key="$$library_key" \
			GlueScriptS3Key="$$script_key" \
			MaximumProjectionPartitions=$(ANALYTICAL_MAX_PROJECTION_PARTITIONS) \
		--tags Project=$(PROJECT_NAME) Component=analytical-projection Environment=$(DEPLOYMENT_ENVIRONMENT) ManagedBy=cloudformation

workflow-infra-deploy: aws-check-account workflow-check-schedule
	@lambda_arn="$$($(AWS_CLI) cloudformation describe-stacks \
		--region $(AWS_REGION) \
		--stack-name $(ACQUISITION_STACK_NAME) \
		--query 'Stacks[0].Outputs[?OutputKey==`AcquisitionLambdaFunctionArn`].OutputValue | [0]' \
		--output text)"; \
	job_name="$$($(AWS_CLI) cloudformation describe-stacks \
		--region $(AWS_REGION) \
		--stack-name $(ANALYTICAL_STACK_NAME) \
		--query 'Stacks[0].Outputs[?OutputKey==`AnalyticalProjectionJobName`].OutputValue | [0]' \
		--output text)"; \
	if [ -z "$$lambda_arn" ] || [ "$$lambda_arn" = "None" ]; then \
		echo "The acquisition Lambda stack output is required" >&2; \
		exit 1; \
	fi; \
	if [ -z "$$job_name" ] || [ "$$job_name" = "None" ]; then \
		echo "The analytical projection Glue job stack output is required" >&2; \
		exit 1; \
	fi; \
	$(AWS_CLI) cloudformation deploy \
		--region $(AWS_REGION) \
		--template-file $(DAILY_WORKFLOW_TEMPLATE) \
		--stack-name $(DAILY_WORKFLOW_STACK_NAME) \
		--capabilities CAPABILITY_IAM \
		--parameter-overrides \
			EnvironmentName=$(DEPLOYMENT_ENVIRONMENT) \
			AcquisitionLambdaArn="$$lambda_arn" \
			AnalyticalProjectionJobName="$$job_name" \
			DailyScheduleExpression="$(DAILY_WORKFLOW_SCHEDULE_EXPRESSION)" \
			DailyScheduleTimezone=$(DAILY_WORKFLOW_SCHEDULE_TIMEZONE) \
			DailyScheduleState=$(DAILY_WORKFLOW_SCHEDULE_STATE) \
		--tags Project=$(PROJECT_NAME) Component=daily-workflow Environment=$(DEPLOYMENT_ENVIRONMENT) ManagedBy=cloudformation

lambda-invoke: aws-check-account
	@mkdir -p $(dir $(ACQUISITION_LAMBDA_RESPONSE_FILE))
	@$(AWS_CLI) lambda invoke \
		--region $(AWS_REGION) \
		--function-name $(PROJECT_NAME)-acquisition-daily-$(DEPLOYMENT_ENVIRONMENT) \
		--cli-binary-format raw-in-base64-out \
		--payload fileb://$(ACQUISITION_LAMBDA_EVENT_FILE) \
		$(ACQUISITION_LAMBDA_RESPONSE_FILE)

glue-start: aws-check-account
	@job_name="$$($(AWS_CLI) cloudformation describe-stacks \
		--region $(AWS_REGION) \
		--stack-name $(ANALYTICAL_STACK_NAME) \
		--query 'Stacks[0].Outputs[?OutputKey==`AnalyticalProjectionJobName`].OutputValue | [0]' \
		--output text)"; \
	$(AWS_CLI) glue start-job-run \
		--region $(AWS_REGION) \
		--job-name "$$job_name"

workflow-start: aws-check-account
	@workflow_arn="$$($(AWS_CLI) cloudformation describe-stacks \
		--region $(AWS_REGION) \
		--stack-name $(DAILY_WORKFLOW_STACK_NAME) \
		--query 'Stacks[0].Outputs[?OutputKey==`DailyWorkflowArn`].OutputValue | [0]' \
		--output text)"; \
	$(AWS_CLI) stepfunctions start-execution \
		--region $(AWS_REGION) \
		--state-machine-arn "$$workflow_arn" \
		--input file://$(abspath $(DAILY_WORKFLOW_INPUT_FILE))
