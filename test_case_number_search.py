from courtreader import readers

reader = readers.DistrictCourtReader()
reader.connect()
reader.get_case_details_by_number('013', 'GC11004355-00')
