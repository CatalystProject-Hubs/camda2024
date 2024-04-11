filenames := eHRs-gen2 diabetes-dualaae-ehr-camda
00_origin_data/unzipped/.success: $(addprefix 00_origin_data/unzipped/, $(addsuffix /.success, $(filenames)))
	touch 00_origin_data/unzipped/.success

00_origin_data/unzipped/%/.success: 00_origin_data/zipped/%.zip
	unzip -d 00_origin_data/unzipped/$* $<
	touch 00_origin_data/unzipped/$*/.success