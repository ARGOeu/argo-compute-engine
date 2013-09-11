Name: ar-compute
Summary: A/R Comp Engine core scripts
Version: 1.0.0
Release: 2%{?dist}
License: ASL 2.0
Buildroot: %{_tmppath}/%{name}-buildroot
Group:     EGI/SA4
BuildArch: noarch
Source0:   %{name}-%{version}.tar.gz
BuildRequires: ant
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

%install 
%{__rm} -rf %{buildroot}
install --directory %{buildroot}/usr/libexec/ar-compute
install --directory %{buildroot}/usr/libexec/ar-compute/pig
install --directory %{buildroot}/var/lib/ar-compute
install --directory %{buildroot}/var/log/ar-compute
#install --directory %{buildroot}/etc/cron.d

install --mode 755 helpers/ar-compute.sh %{buildroot}/usr/libexec/ar-compute
install --mode 644 helpers/InputHandler.pig %{buildroot}/usr/libexec/ar-compute/pig
install --mode 644 status-computation/calculator.pig %{buildroot}/usr/libexec/ar-compute/pig
install --mode 644 status-computation/java/MyUDF.jar %{buildroot}/usr/libexec/ar-compute
#install --mode 644 helpers/ar-cron.sh %{buildroot}/etc/cron.d

%clean
%{__rm} -rf %{buildroot}

%files
%defattr(0644,root,root)
%attr(0755,root,root) /usr/libexec/ar-compute/ar-compute.sh
%attr(0755,root,root) /usr/libexec/ar-compute/pig/InputHandler.pig
%attr(0755,root,root) /usr/libexec/ar-compute/pig/calculator.pig
%attr(0755,root,root) /usr/libexec/ar-compute/MyUDF.jar
#%attr(0755,root,root) /etc/cron.d/ar-cron.sh
%attr(0750,root,root) /var/lib/ar-compute
%attr(0750,root,root) /var/log/ar-compute

%changelog
* Mon Sep 9 2013 Paschalis Korosoglou <pkoro@grid.auth.gr> - 1.0.0-2%{?dist}
- Minor rearrangements of paths
* Thu Aug 29 2013 Paschalis Korosoglou <pkoro@grid.auth.gr> - 1.0.0-1%{?dist}
- Initial release
