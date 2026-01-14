let
  pkgs = import <nixpkgs> { };
  python = import ../../nixfiles/python.nix { pkgs=pkgs; };
  pythonEnv = import ../../nixfiles/uv.nix { pkgs=pkgs; python=python; projectRoot=(../.); };


in pkgs.mkShell { 
  packages = [ 
    pythonEnv
    #rhetenorPackage
    pkgs.uv
    ] ++ (with python.pkgs; [
      matplotlib                                                                
      pandas                                                                    
      numpy                                                                     
      ipython
      jupyter
      seaborn
      mplfinance
      butterflow
      morpho
      unsloth
  ]); 
}

