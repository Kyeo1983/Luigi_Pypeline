#!/bin/bash
source activate pipeline
luigid --background --pidfile ../var/luigi/luigi.pid --logdir ../var/logs/luigi --state-path ../var/luigi/luigi.state
