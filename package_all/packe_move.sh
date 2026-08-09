rm -rf aggregation
mkdir aggregation
cp -r ../core/target/aggregation-bin/** ./aggregation
mkdir aggregation/plugin
cp -r ../data-source-plugins/**/target/aggregation-bin/plugin ./aggregation
cp -r ../job-plugins/**/**/target/aggregation-bin/plugin ./aggregation
cp -r ../file-transfer-plugin/target/aggregation-bin/plugin ./aggregation
mkdir -p aggregation/bin
cp ../file-transfer-cli/target/file-transfer-cli.jar ./aggregation/bin/file-transfer-cli.jar
