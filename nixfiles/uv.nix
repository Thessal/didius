{ pkgs, python, projectRoot } :
let 
  inherit (pkgs) lib;

  pyproject-nix = import (builtins.fetchGit {
    url = "https://github.com/pyproject-nix/pyproject.nix.git";
  }) {
    inherit lib;
  };
  
  project = pyproject-nix.lib.project.loadUVPyproject {
    projectRoot = projectRoot;
  };

  arg = project.renderers.withPackages { inherit python; };
  pythonEnv = python.withPackages arg;
in
pythonEnv
