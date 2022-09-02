{ pkgs ? import (builtins.fetchTarball {
    name = "nixpkgs-22.05pre352484.1882c6b7368";
    url = "https://releases.nixos.org/nixpkgs/nixpkgs-22.05pre352484.1882c6b7368/nixexprs.tar.xz";
    sha256 = "03amwzc9j67c86dmxnknchfk30a63jjky9hhw2wilzdrya07w0kr";
  }) {} }:

let
  jdkVersion = pkgs.openjdk17_headless;
in
  pkgs.mkShell {

    # build-time dependencies
    buildInputs = [
      jdkVersion
      (pkgs.sbt.overrideAttrs (old: old // { jre = jdkVersion.home; }))
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
      export JAVA_HOME='${jdkVersion.home}'
      export SBT_HOME='${pkgs.sbt}'
    '';
  }
