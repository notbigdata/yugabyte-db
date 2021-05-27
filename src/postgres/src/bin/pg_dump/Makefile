#-------------------------------------------------------------------------
#
# Makefile for src/bin/pg_dump
#
# Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
# Portions Copyright (c) 1994, Regents of the University of California
#
# src/bin/pg_dump/Makefile
#
#-------------------------------------------------------------------------

PGFILEDESC = "ysql_dump/pg_restore/ysql_dumpall - backup and restore YSQL databases"
PGAPPICON=win32

subdir = src/bin/pg_dump
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

override CPPFLAGS := -I$(libpq_srcdir) $(CPPFLAGS)
LDFLAGS_INTERNAL += -L$(top_builddir)/src/fe_utils -lpgfeutils $(libpq_pgport)

OBJS=	pg_backup_archiver.o pg_backup_db.o pg_backup_custom.o \
	pg_backup_null.o pg_backup_tar.o pg_backup_directory.o \
	pg_backup_utils.o parallel.o compress_io.o dumputils.o $(WIN32RES)

ifndef YB_BUILD_ROOT
$(error YB_BUILD_ROOT is not set)
endif

#LIBS += -L$(YB_BUILD_ROOT)/lib -lyb_util
# TODO: deduplicate this and the LIBS in postgres/src/backend/Makefile

LIBS += -L$(YB_BUILD_ROOT)/lib \
	-L/opt/yb-build/thirdparty/yugabyte-db-thirdparty-v20210430162751-d7cce0d3e4-centos7-clang11/installed/common/lib \
	-L/opt/yb-build/thirdparty/yugabyte-db-thirdparty-v20210430162751-d7cce0d3e4-centos7-clang11/installed/uninstrumented/lib \
	-Wl,-Bstatic \
	-lyb_pggate -lyb_pggate_util -lconsensus_error -ltablet_error \
	-lyb_client -lmaster_rpc -lyb_partition -ltserver_util -lmaster_util -lmaster_error \
	-lmaster_backup_proto -lmaster_proto -lcdc_consumer_proto -lyb_pgwrapper -lyb_pggate_flags \
	-ltserver_service_proto -lyb_docdb_encoding -lyb_rocksutil -lrocksdb -lsnappy -lz \
	-ltserver_forward_service_proto -lremote_bootstrap_proto -lconsensus_proto \
	-ltserver_admin_proto -lbackup_proto -ltserver_proto -ldocdb_proto -ltablet_proto \
	-lopid_proto -lserver_process -lsqueasel -lserver_common -lyb_fs \
	-lserver_base_proto -lyrpc -lrpc_introspection_proto -lrpc_header_proto -lyb_common \
	-lwire_protocol_proto -lyb_bfpg -luuid -lyb_bfql \
	-lconsensus_metadata_proto -lfs_proto -lpgsql_protocol_proto -lql_protocol_proto \
	-lyb_common_proto -lyb_util -lcrcutil -licudata -lcds-s -licui18n -latomic \
	-lcrypto -licuuc -lcurl -lcrypt_blowfish \
	-lssl -lev -lz -lencryption_proto -lpb_util_proto -lbacktrace \
	-lversion_info_proto -lhistogram_proto -lredis_protocol_proto \
	-lprofiler -lunwind -lprotoc -lgutil -lprotobuf -lglog -lboost_thread -ltcmalloc \
	-lgflags -lboost_system -lldap_r -llber -lc++ -lc++abi \
	-Wl,-Bdynamic -ldl -lrt

all: ysql_dump pg_restore ysql_dumpall

# TODO: this used to include -lyb_pggate -lyb_common after $(LIBS). That means further targets did not need these libs.
ysql_dump: pg_dump.o common.o pg_dump_sort.o $(OBJS) | submake-libpq submake-libpgport submake-libpgfeutils
	$(CC) $(CFLAGS) pg_dump.o common.o pg_dump_sort.o $(OBJS) $(LDFLAGS) $(LDFLAGS_EX) $(LIBS) -o $@$(X)

pg_restore: pg_restore.o $(OBJS) | submake-libpq submake-libpgport submake-libpgfeutils
	$(CC) $(CFLAGS) pg_restore.o $(OBJS) $(LDFLAGS) $(LDFLAGS_EX) $(LIBS) -o $@$(X)

ysql_dumpall: pg_dumpall.o dumputils.o | submake-libpq submake-libpgport submake-libpgfeutils
	$(CC) $(CFLAGS) pg_dumpall.o dumputils.o $(WIN32RES) $(LDFLAGS) $(LDFLAGS_EX) $(LIBS) -o $@$(X)

install: all installdirs
	$(INSTALL_PROGRAM) ysql_dump$(X) '$(DESTDIR)$(bindir)'/ysql_dump$(X)
	$(INSTALL_PROGRAM) pg_restore$(X) '$(DESTDIR)$(bindir)'/pg_restore$(X)
	$(INSTALL_PROGRAM) ysql_dumpall$(X) '$(DESTDIR)$(bindir)'/ysql_dumpall$(X)

installdirs:
	$(MKDIR_P) '$(DESTDIR)$(bindir)'

check:
	$(prove_check)

installcheck:
	$(prove_installcheck)

uninstall:
	rm -f $(addprefix '$(DESTDIR)$(bindir)'/, ysql_dump$(X) pg_restore$(X) ysql_dumpall$(X))

clean distclean maintainer-clean:
	rm -f pg_dump$(X) pg_restore$(X) pg_dumpall$(X) $(OBJS) pg_dump.o common.o pg_dump_sort.o pg_restore.o pg_dumpall.o
	rm -rf tmp_check
