Name: ar-local-compute
Summary: A/R Comp Engine local scripts
Version: 1.3.1
Release: 1%{?dist}
License: ASL 2.0
Buildroot: %{_tmppath}/%{name}-buildroot
Group:     EGI/SA4
BuildArch: x86_64
Source0:   %{name}-%{version}.tar.gz
Requires: ar-sync
Requires: python-pymongo
Requires: hive
Requires: hbase
Requires: hcatalog
Requires: pig
Requires: pig-udf-datafu
Requires: java-1.6.0-openjdk
Conflicts: ar-compute

%description
Installs the local A/R Compute Engine

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


install --mode 755 helpers/ar-local-compute-range.sh                  %{buildroot}/usr/libexec/ar-compute/
install --mode 644 status-computation/local_calculator.pig            %{buildroot}/usr/libexec/ar-compute/pig/
install --mode 644 status-computation/lib/*                           %{buildroot}/usr/libexec/ar-compute/lib/
install --mode 644 status-computation/java/target/MyUDF-1.0.jar       %{buildroot}/usr/libexec/ar-compute/MyUDF.jar
install --mode 644 cronjobs/ar-local-compute-hourly                   %{buildroot}/etc/cron.d
install --mode 644 cronjobs/ar-local-compute-yesterday                %{buildroot}/etc/cron.d
install --mode 644 cronjobs/ar-local-compute-the-day-before-yesterday %{buildroot}/etc/cron.d
install --mode 644 conf/ar-compute-engine.conf                        %{buildroot}/etc/

%clean
cd status-computation/java
mvn clean
%{__rm} -rf %{buildroot}

%files
%defattr(0644,root,root)
%attr(0755,root,root) /usr/libexec/ar-compute/ar-local-compute-range.sh
%attr(0755,root,root) /usr/libexec/ar-compute/pig/local_calculator.pig
%attr(0755,root,root) /usr/libexec/ar-compute/lib/*
%attr(0755,root,root) /usr/libexec/ar-compute/MyUDF.jar
%attr(0750,root,root) /var/lib/ar-compute
%attr(0750,root,root) /var/log/ar-compute
%attr(0755,root,root) /etc/cron.d/ar-local-compute-hourly
%attr(0755,root,root) /etc/cron.d/ar-local-compute-yesterday
%attr(0755,root,root) /etc/cron.d/ar-local-compute-the-day-before-yesterday
%attr(0644,root,root) /etc/ar-compute-engine.conf

%changelog
* Thu Mar 20 2014 Paschalis Korosoglou <pkoro@grid.auth.gr> - 1.3.1-1%{?dist}
- Added hourly and daily cronjobs
* Fri Mar 14 2014 Paschalis Korosoglou <pkoro@grid.auth.gr> - 1.2.2-1%{?dist}
- Added missing dep on pymongo
* Tue Mar 04 2014 Paschalis Korosoglou <pkoro@grid.auth.gr> - 1.2.1-1%{?dist}
- Re-arranged helper scripts
* Thu Feb 06 2014 Paschalis Korosoglou <pkoro@grid.auth.gr> - 1.1.0-1%{?dist}
- Fixed issue in pig comments
* Tue Jan 14 2014 Paschalis Korosoglou <pkoro@grid.auth.gr> - 1.0.18-3%{?dist}
- Connection with MongoDB
* Tue Jan 14 2014 Paschalis Korosoglou <pkoro@grid.auth.gr> - 1.0.18-1%{?dist}
- Initial package
