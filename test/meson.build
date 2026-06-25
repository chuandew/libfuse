# Compile helper programs
td = []
foreach prog: [ 'test_write_cache', 'test_setattr', 'hello' ]
    td += executable(prog, prog + '.c',
                     include_directories: include_dirs,
                     link_with: [ libfuse ],
                     dependencies: thread_dep,
                     install: false)
endforeach
td += executable('test_syscalls', 'test_syscalls.c',
                 include_directories: include_dirs,
                 install: false)
td += executable('readdir_inode', 'readdir_inode.c',
                 include_directories: include_dirs,
                 install: false)
td += executable('release_unlink_race', 'release_unlink_race.c',
                 dependencies: [ libfuse_dep ],
                 install: false)
td += executable('test_want_conversion', 'test_want_conversion.c',
                 dependencies: [ libfuse_dep ],
                 install: false)
td += executable('test_signals', 'test_signals.c',
                 dependencies: [ libfuse_dep, thread_dep ],
                 install: false)
td += executable('test_abi', 'test_abi.c',
                 dependencies: [ libfuse_dep ],
                 install: false)
td += executable('test_wait_drained_timeout', 'test_wait_drained_timeout.c',
                 dependencies: [ libfuse_dep, thread_dep ],
                 install: false)
td += executable('test_received_inflight', 'test_received_inflight.c',
                 dependencies: [ libfuse_dep, thread_dep ],
                 install: false)
td += executable('test_drop_race', 'test_drop_race.c',
                 dependencies: [ libfuse_drainhooks_dep, thread_dep ],
                 c_args: [ '-DFUSE_TEST_DRAIN_HOOKS' ],
                 install: false)
td += executable('test_drain_spawn_race', 'test_drain_spawn_race.c',
                 dependencies: [ libfuse_drainhooks_dep, thread_dep ],
                 c_args: [ '-DFUSE_TEST_DRAIN_HOOKS' ],
                 install: false)
td += executable('test_drain_cancel_safety', 'test_drain_cancel_safety.c',
                 dependencies: [ libfuse_drainhooks_dep, thread_dep ],
                 c_args: [ '-DFUSE_TEST_DRAIN_HOOKS' ],
                 install: false)
td += executable('test_drain_handoff', 'test_drain_handoff.c',
                 dependencies: [ libfuse_dep, thread_dep ],
                 install: false)
td += executable('test_reset_clears_drain', 'test_reset_clears_drain.c',
                 dependencies: [ libfuse_dep, thread_dep ],
                 install: false)

test_scripts = [ 'conftest.py', 'pytest.ini', 'test_examples.py',
                 'util.py', 'test_ctests.py', 'test_custom_io.py' ]
td += custom_target('test_scripts', input: test_scripts,
                      output: test_scripts, build_by_default: true,
                      command: ['cp', '-fPp',
                                '@INPUT@', meson.current_build_dir() ])

# Provide something helpful when running 'ninja test'

if meson.is_subproject()
	test('libfuse is a subproject, skipping tests', executable('wrong_command',
                      'wrong_command.c', install: false,
                       c_args: [ '-DMESON_IS_SUBPROJECT' ]))
else
	test('wrong_command', executable('wrong_command', 'wrong_command.c',
                      install: false))
endif
