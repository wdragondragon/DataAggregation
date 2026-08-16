rm -rf aggregation
mkdir aggregation
cp -r ../core/target/aggregation-bin/** ./aggregation
mkdir aggregation/plugin
cp -r ../data-source-plugins/**/target/aggregation-bin/plugin ./aggregation
cp -r ../job-plugins/**/**/target/aggregation-bin/plugin ./aggregation