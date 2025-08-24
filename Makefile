# ======================================================
# Execute Step Function Pipeline
# ======================================================
execute-pipeline:
	@echo "🚀 Starting InsightFlow Data Pipeline..."
	@cd terraform/dev && \
	ARN=$$(terraform output -raw step_functions_state_machine_arn 2>/dev/null) && \
	cd ../.. && \
	if [ -z "$$ARN" ]; then \
		echo "❌ Error: No Step Functions ARN found. Run 'make apply' first." && \
		exit 1; \
	fi && \
	echo "📋 Starting execution..." && \
	EXECUTION_ARN=$$(aws stepfunctions start-execution \
		--state-machine-arn "$$ARN" \
		--name "pipeline-$$(date +%s)" \
		--query 'executionArn' \
		--output text) && \
	echo "⏳ Waiting for completion..." && \
	while [ "$$(aws stepfunctions describe-execution --execution-arn "$$EXECUTION_ARN" --query 'status' --output text)" = "RUNNING" ]; do \
		echo "⏳ Running... $$(date)"; \
		sleep 30; \
	done && \
	STATUS=$$(aws stepfunctions describe-execution --execution-arn "$$EXECUTION_ARN" --query 'status' --output text) && \
	if [ "$$STATUS" = "SUCCEEDED" ]; then \
		echo "✅ Pipeline completed successfully!"; \
	else \
		echo "❌ Pipeline failed with status: $$STATUS" && \
		aws stepfunctions describe-execution --execution-arn "$$EXECUTION_ARN" && \
		exit 1; \
	fi