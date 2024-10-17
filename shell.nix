{ pkgs ? import <nixpkgs> { } }:
with pkgs;


mkShell {
  hardeningDisable = [ "all" ]; # https://github.com/go-delve/delve/issues/3085
  buildInputs = [
    pkgs.go
  ];
}