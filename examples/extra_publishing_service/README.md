# Overview

This service do take messages from one queue, but will
write output messages to 2 different queues using standard
service output (`return` output) for one queue and predefined
publisher for publishing to other queue.