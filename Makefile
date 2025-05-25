.PHONY: docker-build

.PHONY: tar-libs
tar-libs:
	tar --exclude '__pycache__' -czvf packages/ifsutils.tgz -C packages/ifsutils/ ifsutils

.PHONY: docker-build
docker-build: tar-libs
	@echo "\033[0;32mBuilding iceberg base image \033[0m"
	@docker build -f Dockerfile --tag $$(cat manifest.yaml | yq .destination_repo_base):$$(cat manifest.yaml | yq .version) .
	@docker tag $$(cat manifest.yaml | yq .destination_repo_base):$$(cat manifest.yaml | yq .version) $(REPO_URL)/$$(cat manifest.yaml | yq .destination_repo_base):$$(cat manifest.yaml | yq .version)
	@docker push $(REPO_URL)/$$(cat manifest.yaml | yq .destination_repo_base):$$(cat manifest.yaml | yq .version)

.PHONY: docker-build-test
docker-build-test: tar-libs
	@echo "\033[0;32mBuilding iceberg test image \033[0m"
	@docker build -f Dockerfile.test --tag $$(cat manifest.yaml | yq .destination_repo_test):$$(cat manifest.yaml | yq .version) .
	@docker tag $$(cat manifest.yaml | yq .destination_repo_test):$$(cat manifest.yaml | yq .version) $(REPO_URL)/$$(cat manifest.yaml | yq .destination_repo_test):$$(cat manifest.yaml | yq .version)
	@docker push $(REPO_URL)/$$(cat manifest.yaml | yq .destination_repo_test):$$(cat manifest.yaml | yq .version)
