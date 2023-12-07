all: kill run

run:
	supervisord
	python3 src/main.py

# Kill supervisord if running (which would be independent of the current window)
# https://stackoverflow.com/questions/7394290/how-to-check-return-value-from-the-shell-directive
# https://unix.stackexchange.com/questions/119648/redirecting-to-dev-null
# https://stackoverflow.com/questions/14479894/stopping-supervisord-shut-down
SUPERVISORD_RETVAL := $(shell supervisorctl pid > /dev/null 2>&1; echo $$?)
SUPERVISORD_PID := $(shell supervisorctl pid)
kill:
    ifeq ($(SUPERVISORD_RETVAL),0)
	kill -s SIGTERM $(SUPERVISORD_PID)
    else
	@echo "supervisorctl returned nonzero error code, likely not running"
    endif

# Install redis, then install pip requirements
# https://redis.io/docs/install/install-redis/install-redis-on-linux/
# Adding the repo just doesn't work for some reason, but it does seem to be available
# on the standard package distributors.
deps:
#	curl -fsSL https://packages.redis.io/gpg | sudo gpg --dearmor -o /usr/share/keyrings/redis-archive-keyring.gpg

#	echo "deb [signed-by=/usr/share/keyrings/redis-archive-keyring.gpg] https://packages.redis.io/deb $(shell lsb_release -cs) main" | sudo tee /etc/apt/sources.list.d/redis.list
	sudo apt-get update
	sudo apt-get install redis
	pip install -r requirements.txt


