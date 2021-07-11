#!/usr/bin/env escript

%% this script forwards the call to brod_cli module
%% we avoid using the escriptised file due to the need of loading nif binaries

main(Args) ->
    ok = add_libs_dir(),
    brod_cli:main(Args).

add_libs_dir() ->
    %% RELEASE_ROOT_DIR is set by bord-i boot script
    [_ | _] = RootDir = os:getenv("RELEASE_ROOT_DIR"),
    RelFile = filename:join([RootDir, "releases", "RELEASES"]),
    case file:consult(RelFile) of
        {ok, [Releases]} ->
            Release = lists:keyfind("i", 3, Releases),
            {release, _Name, _AppVsn, _ErtsVsn, Libs, _State} = Release,
            lists:foreach(
              fun({Name, Vsn, _}) ->
                      add_lib_dir(RootDir, Name, Vsn)
              end, Libs);
        {error, Reason} ->
            error({failed_to_read_RELEASES_file, RelFile, Reason})
    end.

add_lib_dir(RootDir, Name, Vsn) ->
    LibDir = filename:join([RootDir, lib, atom_to_list(Name) ++ "-" ++ Vsn, ebin]),
    case code:add_patha(LibDir) of
        true -> ok;
        {error, _} -> error(LibDir)
    end.
