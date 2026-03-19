from __future__ import absolute_import
from __future__ import print_function
from courtreader import readers
import six

print('CIRCUIT COURT')
reader = readers.CircuitCourtReader()
courts = reader.connect()
court_names = []
for fips_code, court in six.iteritems(courts):
    court_names.append(court['name'] + ' ' + fips_code)
court_names.sort()
for court_name in court_names:
    print(court_name)

print('DISTRICT COURT')
reader = readers.DistrictCourtReader()
courts = reader.connect()
court_names = []
for fips_code, court in six.iteritems(courts):
    court_names.append(court + ' ' + fips_code)
court_names.sort()
for court_name in court_names:
    print(court_name)
