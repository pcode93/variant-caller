# variant-caller
A simple distributed variant caller built using Apache Spark and ADAM.
#How to run
variant-caller.sh &lt;path to reference&gt; &lt;path to alignment&gt; &lt;output path&gt; [options]
###Options
Name | Description
---- | -----------
-homozygousThreshold | Sets the homozygous thershold
-heterozygousThreshold | Sets the heterozygous threshold
-mapq | Filters reads based on the given mapq value
-debug | Turns on debug logging
