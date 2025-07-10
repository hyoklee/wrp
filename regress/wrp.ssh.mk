.PHONY: all $(HOSTS)

# Define your SSH hosts here (e.g., user@hostname or user@IP)
HOSTS := jam nene guanaco alpaca giraffe

# Define the command to execute on remote hosts
COMMAND ?= "wrt"

# Default target: depends on all host-specific targets
all: $(HOSTS)

# Rule to define how to execute a command on a single host
# The $@ variable represents the current target (which will be a host name)
%:
	@echo "--- Connecting to $@ ---"
	@ssh -T $@ $(COMMAND) put $@.yml
	@echo "--- Command completed on $@ ---"
