from courtreader import readers

reader = readers.DistrictCourtReader()
reader.connect()
print reader.get_case_details_by_number('015', 'GT14009464-00')
