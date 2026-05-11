N       ?= 4
F       ?= 1
SESSION := byzantine
DIR     := $(CURDIR)

.PHONY: run stop

run:
	rm -f /tmp/.byz_done_*
	tmux new-session -d -s $(SESSION)
	tmux send-keys -t $(SESSION) "cd $(DIR) && go run . --addr /ip4/0.0.0.0/tcp/9001 --id 1 --n $(N) --f $(F); touch /tmp/.byz_done_1" Enter
	tmux split-window -v -t $(SESSION)
	tmux send-keys -t $(SESSION) "cd $(DIR) && go run . --addr /ip4/0.0.0.0/tcp/9002 --id 2 --n $(N) --f $(F); touch /tmp/.byz_done_2" Enter
	tmux split-window -v -t $(SESSION)
	tmux send-keys -t $(SESSION) "cd $(DIR) && go run . --addr /ip4/0.0.0.0/tcp/9003 --id 3 --n $(N) --f $(F); touch /tmp/.byz_done_3" Enter
	tmux split-window -v -t $(SESSION)
	tmux send-keys -t $(SESSION) "cd $(DIR) && go run . --addr /ip4/0.0.0.0/tcp/9004 --id 4 --n $(N) --f $(F); touch /tmp/.byz_done_4" Enter
	tmux new-window -t $(SESSION)
	tmux send-keys -t $(SESSION) "while [ \$$(ls /tmp/.byz_done_* 2>/dev/null | wc -l) -lt $(N) ]; do sleep 1; done; sleep 5; tmux kill-session -t $(SESSION)" Enter
	tmux select-window -t $(SESSION):0
	tmux select-layout -t $(SESSION) tiled
	tmux attach-session -t $(SESSION)

stop:
	tmux kill-session -t $(SESSION) 2>/dev/null || true
