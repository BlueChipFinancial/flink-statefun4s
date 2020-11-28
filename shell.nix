{ pkgs ? import (builtins.fetchTarball {
    name = "nixos-unstable-2010-07-08";
    url = "https://github.com/nixos/nixpkgs/archive/1ceecdd109d0c30fd48fc6af295d9c8b05d6c6c6.tar.gz";
    sha256 = "10bjz6c47g7k1pxlbxh37dbjbhmywy1l1l02i6r2r5zkbf9ypv53";
  }) {} }:

let
  jdkVersion = pkgs.openjdk8;
in
  pkgs.mkShell {

    # build-time dependencies
    buildInputs = [
      jdkVersion
      pkgs.sbt
      pkgs.scalafmt
      pkgs.jekyll
      pkgs.git
      pkgs.ncurses
      pkgs.envsubst
    ];

    # Runtime dependencies
    propagatedBuildInputs = [
      # C libraries required to build
    ];


    shellHook = ''
      export JAVA_HOME='${jdkVersion}'
      export SBT_HOME='${pkgs.sbt}'
    '';
  }
