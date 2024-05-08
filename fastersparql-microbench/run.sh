#!/bin/bash
ORIG_PATH="$PATH"
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"

function run() {
	if [ ! -d "$1" ] || [ ! -x "$1/bin/java" ]; then
		echo "No java at \"$1/bin/java\"" 1>&2
		return 1
	fi
	export JAVA_HOME=$1
	export PATH=$JAVA_HOME/bin:$ORIG_PATH
	set -e
	(cd "$DIR/.." && mvn package -DskipTests=true)
	sleep 5s
	cd "$DIR"
	java --enable-preview --add-modules jdk.incubator.vector \
		-Dfs.data.dir=/home/alexis/linked-data/freqel-benchmark-data/downloads \
		-Dfastersparql.emit.log-stats=false \
		-Dfastersparql.vectorization=true \
		-Dfastersparql.unsafe=true \
		-jar target/benchmarks.jar \
		com.github.alexishuf.fastersparql.lrb.QueryBench.termLen.* \
		-p queries=S.* \
		-p srcKind=FS_STORE \
		-p flowModel=EMIT \
		-p selKind=ASK -p crossSourceDedup=false -p weakenDistinct=true \
		-p batchKind=COMPRESSED \
		-f 1 | tee "$(echo $JAVA_HOME | sed -E 's@.*/@@g').txt"
	#-prof async:event=cpu;output=jfr;libPath=/home/alexis/apps/async-profiler/lib/libasyncProfiler.so
}

run "$HOME/apps/jdk-21+35-gcc-znver2-O3"
sleep 10s
run "$HOME/apps/jdk-21+35-gcc-znver2"
sleep 10s
run "$HOME/apps/jdk-21"
