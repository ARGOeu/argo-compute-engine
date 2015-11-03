Name: ar-compute
Summary: A/R Comp Engine core scripts
Version: 1.6.5
Release: 2%{?dist}
License: ASL 2.0
Buildroot: %{_tmppath}/%{name}-buildroot
Group:     EGI/SA4
BuildArch: x86_64
Source0:   %{name}-%{version}.tar.gz
Requires: python-argparse
Requires: python-pymongo
Requires: hive
Requires: hbase
Requires: hcatalog
Requires: pig
Requires: pig-udf-datafu
Requires: java-1.7.0-openjdk

%description
Installs the core A/R Compute Engine

%prep
%setup 
cd status-computation/java
mvn package

%install 
%{__rm} -rf %{buildroot}
install --directory %{buildroot}/usr/libexec/ar-compute
install --directory %{buildroot}/usr/libexec/ar-compute/pig
install --directory %{buildroot}/usr/libexec/ar-compute/lib
install --directory %{buildroot}/usr/libexec/ar-compute/lib/avro
install --directory %{buildroot}/usr/libexec/ar-compute/bin
install --directory %{buildroot}/var/lib/ar-compute
install --directory %{buildroot}/var/log/ar-compute
install --directory %{buildroot}/etc
install --directory %{buildroot}/etc/ar-compute

install --mode 755 helpers/ar-compute.py                        %{buildroot}/usr/libexec/ar-compute/
install --mode 644 status-computation/pig/*                     %{buildroot}/usr/libexec/ar-compute/pig/
install --mode 644 status-computation/lib/avro/*                %{buildroot}/usr/libexec/ar-compute/lib/avro/
install --mode 644 status-computation/lib/*.jar                 %{buildroot}/usr/libexec/ar-compute/lib/
install --mode 644 status-computation/lib/*.sh                  %{buildroot}/usr/libexec/ar-compute/lib/
install --mode 644 status-computation/lib/*.py                  %{buildroot}/usr/libexec/ar-compute/lib/
install --mode 755 bin/*.py                                     %{buildroot}/usr/libexec/ar-compute/bin/
install --mode 644 status-computation/java/target/MyUDF-1.0.jar %{buildroot}/usr/libexec/ar-compute/MyUDF.jar
install --mode 644 conf/ar-compute-engine.conf                  %{buildroot}/etc/
install --mode 644 conf/*.json                                  %{buildroot}/etc/ar-compute

%clean
cd status-computation/java
mvn clean
%{__rm} -rf %{buildroot}

%files
%defattr(0644,root,root)
%attr(0755,root,root) /usr/libexec/ar-compute/ar-compute.p*
%attr(0755,root,root) /usr/libexec/ar-compute/pig/*.pig
%attr(0755,root,root) /usr/libexec/ar-compute/lib/*
%attr(0755,root,root) /usr/libexec/ar-compute/bin/*.py
%attr(0755,root,root) /usr/libexec/ar-compute/MyUDF.jar
%attr(0750,root,root) /var/lib/ar-compute
%attr(0750,root,root) /var/log/ar-compute
%attr(0644,root,root) /etc/ar-compute-engine.conf
%attr(0644,root,root) /etc/ar-compute/*.json

%changelog
* Tue Nov 3 2015 Konstantinos Kagkelidis <kaggis@gmail.com> - 1.6.5-2%{?dist}
- ARGO-245 Upgrade devel to CDH 5.x - Update spec requirement to java-1.7.0-openjdk
* Thu Oct 29 2015 Konstantinos Kagkelidis <kaggis@gmail.com> - 1.6.5-1%{?dist}
- ARGO-207 minor bug fix related to how mongo_clean_status is called from within job_status_detail python wrapper
- ARGO-211 Use ListArray assertions instead of plain asserts when dealing with arraylists.
Add extra junit dependencies
- ARGO-211 Add report field also. Change Pig script to handle newly exposed fields
- ARGO-212 Reintroduce continuous timelines
- ARGO-212 Implement more robust continuous timeline
- ARGO-212 Change v. in joda time dependency
- ARGO-212 Prepare pig data flow for endpoint aggr
- ARGO-212 fix newlines at end of files
- ARGO-201 Add multitenant info to cli doc
- ARGO-201 Remove deprecated datastore mapping parameters. No longer needed in multitenant env
- ARGO-215 Change UDF output schema to be compatible with pig 12 onwards(CDH 5.x)
- ARGO-247 Update recomputation profile definition
- ARGO-247 support multiple periods of recomputation for the same endpoint group
* Fri Aug 7 2015 Konstantinos Kagkelidis <kaggis@gmail.com> - 1.6.4-1%{?dist}
- ARGO-146 Implement multi-tenancy required changes for the CE
* Tue Jul 21 2015 Konstantinos Kagkelidis <kaggis@gmail.com> - 1.6.2-7%{?dist}
- ARGO-190 Recomputation exclude list fix
* Tue Jun 30 2015 Avraam Tsantekidis <avraamt@grid.auth.gr> - 1.6.2-6%{?dist}
- ARGO-153 fix in call for recomputation script from poller
* Tue Jun 23 2015 Paschalis Korosoglou <pkoro@grid.auth.gr> - 1.6.2-5%{?dist}
- Removed one threshold check
- Fix issue with non Json parsable datetime object
* Wed Jun 16 2015 Paschalis Korosoglou <pkoro@grid.auth.gr> - 1.6.2-4%{?dist}
- Fix in parameter passed onto pig ar script 
* Wed Jun 4 2015 Konstantinos Kagkelidis <kaggis@gmail.com> - 1.6.2-3%{?dist}
- ARGO-129 Bugfix: Fix reference to connector path
* Wed Jun 3 2015 Konstantinos Kagkelidis <kaggis@gmail.com> - 1.6.2-2%{?dist}
- ARGO-74 Implement recomputation execution
- ARGO-75 Implement changes in recomputation state
- Further changes in poller implementation
* Thu May 28 2015 Avraam Tsantekidis <avraamt@grid.auth.gr> - 1.6.2-1%{?dist}
- ARGO-125 Added test for raising TypeError
- ARGO-125 changed assertion to exception
- ARGO-125 Changed 'none' to None for log_file name
* Thu May 14 2015 Konstantinos Kagkelidis <kaggis@gmail.com> - 1.6.1-5%{?dist}
- ARGO-118 Reading new avro "tags" field type as map
- ARGO-48 Add logging to the data model classes
- ARGO-47 Added exception handling in AR UDFs
- ARGO-46 logging in java sync classes
- ARGO-88 Log info about argo mode (local/cluster) during operations
* Wed Apr 15 2015 Konstantinos Kagkelidis <kaggis@gmail.com> - 1.6.1-4%{?dist}
- Multitenancy support in configuration files (derived from ARGO-67)
* Tue Apr 14 2015 Paschalis Korosoglou <pkoro@grid.auth.gr> - 1.6.1-3%{?dist}
- ARGO-61 Updates in python wrappers to make sync path configurable in CE
* Thu Apr 9 2015 Paschalis Korosoglou <pkoro@grid.auth.gr> - 1.6.1-2%{?dist}
- Removed runtime dependancy on ar-sync package
* Thu Mar 26 2015 Paschalis Korosoglou <pkoro@grid.auth.gr> - 1.6.1-1%{?dist}
- Renameing of standalone folder to bin
* Mon Mar 02 2015 Konstantinos Kagkelidis <kaggis@gmail.com> - 1.6.0-6%{?dist}
- Fix typo in ar-compute-engine.conf 
* Fri Feb 27 2015 Konstantinos Kagkelidis <kaggis@gmail.com> - 1.6.0-5%{?dist}
- Fix config filenames. Add More Verbosity
- Correct Cloudmon job name in global config
- Fix minor issues add more verbosity
- Add forgotten default_unknown entry from ops file
- Optimize filtering of metric data by using also availability profile info
- Minor changes to pig scripts for optimization of metric data filtering
- Add some cleanup paramateres to ar-compute-engine.conf
- Add Verbosity,clean up parameters and minor fixes to init scripts
- Fix weight file to be referenced from inside job folder
- Init script fixes
- Add abil. to lookback up to 5 days for sync files in job folders
- Simplify job daily cycle script
- Add script to archive monthly sync data to hdfs (per tenant/all jobs)
* Tue Feb 24 2015 Konstantinos Kagkelidis <kaggis@gmail.com> - 1.6.0-3%{?dist}
- Whitespace fix
- Get group name in which service belongs in a Av.Profile
- Fix error retreaving Group Operation
- Optimizations, Date to slot calculation fixes, expanded unit tests
- Remove Unused Imports
- Minor Refactoring
- Add startState Parameter, Expand Tests
- Fix insconsistencies to honor new spec deployment
- Implement fixes for standalone version
* Fri Feb 06 2015 Konstantinos Kagkelidis <kaggis@gmail.com> - 1.6.0-2%{?dist}
- Add Support for Modular Compuation Elements
* Tue Feb 03 2015 Konstantinos Kagkelidis <kaggis@gmail.com> - 1.6.0-1%{?dist}
- Add support for Custom Metric Statuses, Operations and Availability Profiles
* Fri Jan 16 2015 Konstantinos Kagkelidis <kaggis@gmail.com> - 1.6.0-0%{?dist}
- Adding support for reading avro files from consumer and sync components
* Wed Jan 14 2015 Konstantinos Kagkelidis <kaggis@gmail.com> - 1.5.0.-3%{?dist}
- Add support for Previous timestamp. Various Fixes
* Wed Dec 17 2014 Konstantinos Kagkelidis <kaggis@gmail.com> - 1.5.0-2%{?dist}
- Add support for producing status result aggregations.
* Wed Dec 03 2014 Konstantinos Kagkelidis <kaggis@gmail.com> - 1.5.0-1%{?dist}
- Add support for producing status results. Add Support for handling avro files
* Thu Nov 13 2014 Paschalis Korosoglou <pkoro@grid.auth.gr> - 1.4.6-1%{?dist}
- Removal of depricated call in calculator.sh
* Tue Jul 22 2014 Konstantinos Kagkelidis <kaggis@gmail.com> - 1.4.5-1%{?dist}
- Mongo field schema changes (date field: d->dt)
* Fri May 02 2014 Paschalis Korosoglou <pkoro@grid.auth.gr> - 1.4.4-1%{?dist}
- Re-organization of cron jobs and addition of monthly cron
* Mon Apr 28 2014 Paschalis Korosoglou <pkoro@grid.auth.gr> - 1.4.3-1%{?dist}
- Added logging to ar-compute cron scripts
* Mon Apr 14 2014 Paschalis Korosoglou <pkoro@grid.auth.gr> - 1.4.2-1%{?dist}
- Fix in sfreports deletion
* Tue Apr 01 2014 Paschalis Korosoglou <pkoro@grid.auth.gr> - 1.4.1-1%{?dist}
- Checks added for hepspec, topology and poem_sync files extistence on consumer node
- Simplified cronjobs
* Sat Mar 22 2014 Anastasios Andronidis <andronat@grid.auth.gr> - 1.4.0-1%{?dist}
- New script stracture. Added python-argparse dep
* Thu Mar 20 2014 Paschalis Korosoglou <pkoro@grid.auth.gr> - 1.3.1-1%{?dist}
- Added hourly and daily cronjobs
* Fri Mar 14 2014 Paschalis Korosoglou <pkoro@grid.auth.gr> - 1.2.2-1%{?dist}
- Added missing dep on pymongo
* Tue Mar 04 2014 Paschalis Korosoglou <pkoro@grid.auth.gr> - 1.2.1-1%{?dist}
- Re-arranged helper scripts
* Thu Feb 21 2014 Paschalis Korosoglou <pkoro@grid.auth.gr> - 1.2.0-1%{?dist}
- Re-arranged helper scripts and cron jobs
* Thu Feb 06 2014 Paschalis Korosoglou <pkoro@grid.auth.gr> - 1.1.0-1%{?dist}
- Fixed issue in pig comments
* Mon Dec 02 2013 Paschalis Korosoglou <pkoro@grid.auth.gr> - 1.0.16-4%{?dist}
- Updated cronjob
* Fri Nov 29 2013 Paschalis Korosoglou <pkoro@grid.auth.gr> - 1.0.16-3%{?dist}
- Updated cronjob
* Thu Nov 28 2013 Paschalis Korosoglou <pkoro@grid.auth.gr> - 1.0.16-2%{?dist}
- Added cronjob for input files
* Mon Sep 9 2013 Paschalis Korosoglou <pkoro@grid.auth.gr> - 1.0.0-2%{?dist}
- Minor rearrangements of paths
* Thu Aug 29 2013 Paschalis Korosoglou <pkoro@grid.auth.gr> - 1.0.0-1%{?dist}
- Initial release
