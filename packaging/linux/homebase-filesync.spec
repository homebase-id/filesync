# The .rpm (Fedora, openSUSE): the same layout as Conveyor's .deb, from the release's staged
# programs (static, so no library dependencies). No update repository yet: updates by hand.
# rpmbuild -bb --define "pkg_version 0.1.0" --define "stage <dir with bin/>" --define "src <repo>"
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
lib=%{buildroot}/usr/lib/homebase/homebase-filesync/bin
install -d $lib %{buildroot}/usr/bin %{buildroot}/usr/share/applications
install -m 0755 %{stage}/bin/homebase-filesync %{stage}/bin/hbsyncd %{stage}/bin/hbsync $lib/
ln -s ../lib/homebase/homebase-filesync/bin/homebase-filesync %{buildroot}/usr/bin/homebase-filesync
ln -s ../lib/homebase/homebase-filesync/bin/hbsync %{buildroot}/usr/bin/hbsync
install -m 0644 %{src}/packaging/linux/id.homebase.filesync.desktop %{buildroot}/usr/share/applications/
for size in 64 256; do
  install -D -m 0644 %{src}/crates/hbsync-tray/assets/mark-$size.png \
    %{buildroot}/usr/share/icons/hicolor/${size}x${size}/apps/homebase-filesync.png
done
install -D -m 0644 %{src}/LICENSE %{buildroot}/usr/share/licenses/homebase-filesync/LICENSE

%files
/usr/lib/homebase
/usr/bin/homebase-filesync
/usr/bin/hbsync
/usr/share/applications/id.homebase.filesync.desktop
/usr/share/icons/hicolor/64x64/apps/homebase-filesync.png
/usr/share/icons/hicolor/256x256/apps/homebase-filesync.png
/usr/share/licenses/homebase-filesync/LICENSE
