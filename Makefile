main: *.sml *.mlb hash-table/*.sml hash-table/*.mlb
	mpl -default-type int64 -default-type word64 main.mlb