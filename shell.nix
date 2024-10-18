{ pkgs ? import <nixpkgs> { } }:
with pkgs;


mkShell {
  hardeningDisable = [ "all" ]; # https://github.com/go-delve/delve/issues/3085
  buildInputs = [
    pkgs.go

    # Use to test avro to POJO generation
    pkgs.wget
    pkgs.temurin-bin
  ];
}