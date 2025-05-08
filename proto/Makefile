# Find all .proto files in the current directory
PROTO_FILES := $(wildcard *.proto)

# Define the target files as .pb.go files
GO_TARGETS := $(PROTO_FILES:.proto=.pb.go)

# Define the target files for documentation as HTML files in the doc directory
# Note: Must do `go install github.com/pseudomuto/protoc-gen-doc/cmd/protoc-gen-doc@latest`
DOC_TARGETS := $(PROTO_FILES:.proto=.html)

# Default target
all: $(GO_TARGETS)

# Rule to generate .pb.go from .proto
%.pb.go: %.proto
	protoc --go_out=../common/proto --twirp_out=../common/proto --experimental_allow_proto3_optional $<

# Rule to generate HTML documentation from .proto
doc/%.html: %.proto
	@mkdir -p doc
	protoc --doc_out=../docs --doc_opt=html,$@ $<

# Target for documentation
docs: $(DOC_TARGETS)

# Phony target for clean to remove generated .pb.go files and documentation
.PHONY: clean
clean:
	rm -f $(GO_TARGETS) $(DOC_TARGETS)
