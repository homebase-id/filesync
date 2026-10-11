# The .rpm (Fedora, openSUSE): Conveyor's .deb, its files as they are, so the layout comes from
# packaging/conveyor.conf alone (without the apt repository the .deb adds under /etc). The
# programs are static: no library dependencies. No update repository yet: updates by hand.
# rpmbuild -bb --define "pkg_version 0.1.0" --define "root <the .deb's files>" --define "src <repo>"
Name:           homebase-filesync
Version:        %{pkg_version}
Release:        1
Summary:        Keeps your Homebase folder in sync
License:        AGPL-3.0-only
URL:            https://github.com/homebase-id/filesync
BuildArch:      x86_64
Recommends:     xdg-desktop-portal

# Release builds, already stripped: nothing for rpmbuild to extract or strip.
%global debug_package %{nil}
%global __strip /bin/true

%description
Homebase Filesync keeps a folder on this computer in sync with your Homebase drive: a tray
app, a per-user daemon (hbsyncd) and a command line (hbsync).

%install
cp -a %{root}/usr %{buildroot}/
install -D -m 0644 %{src}/LICENSE %{buildroot}/usr/share/licenses/homebase-filesync/LICENSE

%files
/usr/lib/homebase
/usr/bin/homebase-filesync
/usr/bin/hbsync
/usr/share/applications/id.homebase.filesync.desktop
/usr/share/icons/hicolor/*/apps/homebase-filesync.png
/usr/share/metainfo/id.homebase.filesync.metainfo.xml
/usr/share/licenses/homebase-filesync/LICENSE
