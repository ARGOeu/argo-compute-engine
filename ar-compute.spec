Name: ar-compute
Summary: A/R Comp Engine core scripts
Version: 1.4.2
Release: 1%{?dist}
License: ASL 2.0
Buildroot: %{_tmppath}/%{name}-buildroot
Group:     EGI/SA4
BuildArch: x86_64
Source0:   %{name}-%{version}.tar.gz
Requires: ar-sync
Requires: python-argparse
Requires: python-pymongo
Requires: hive
Requires: hbase
Requires: hcatalog
Requires: pig
Requires: pig-udf-datafu
Requires: java-1.6.0-openjdk

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
install --directory %{buildroot}/var/lib/ar-compute
install --directory %{buildroot}/var/log/ar-compute
install --directory %{buildroot}/etc
install --directory %{buildroot}/etc/cron.d

install --mode 755 helpers/ar-compute.py                        %{buildroot}/usr/libexec/ar-compute/
install --mode 644 status-computation/pig/*                     %{buildroot}/usr/libexec/ar-compute/pig/
install --mode 644 status-computation/lib/*                     %{buildroot}/usr/libexec/ar-compute/lib/
install --mode 644 status-computation/java/target/MyUDF-1.0.jar %{buildroot}/usr/libexec/ar-compute/MyUDF.jar
install --mode 644 cronjobs/ar-compute-hourly                   %{buildroot}/etc/cron.d
install --mode 644 cronjobs/ar-compute-yesterday                %{buildroot}/etc/cron.d
install --mode 644 cronjobs/ar-compute-the-day-before-yesterday %{buildroot}/etc/cron.d
install --mode 644 conf/ar-compute-engine.conf                  %{buildroot}/etc/

%clean
cd status-computation/java
mvn clean
%{__rm} -rf %{buildroot}

%files
%defattr(0644,root,root)
%attr(0755,root,root) /usr/libexec/ar-compute/ar-compute.py
%attr(0755,root,root) /usr/libexec/ar-compute/pig/calculator.pig
%attr(0755,root,root) /usr/libexec/ar-compute/pig/local_calculator.pig
%attr(0755,root,root) /usr/libexec/ar-compute/lib/*
%attr(0755,root,root) /usr/libexec/ar-compute/MyUDF.jar
%attr(0750,root,root) /var/lib/ar-compute
%attr(0750,root,root) /var/log/ar-compute
%attr(0755,root,root) /etc/cron.d/ar-compute-hourly
%attr(0755,root,root) /etc/cron.d/ar-compute-yesterday
%attr(0755,root,root) /etc/cron.d/ar-compute-the-day-before-yesterday
%attr(0644,root,root) /etc/ar-compute-engine.conf

%changelog
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
