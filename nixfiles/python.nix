{ pkgs } : 
pkgs.python313.override {
  packageOverrides = self: super: {
    butterflow = self.buildPythonPackage rec {
      pname = "butterflow";
      version = "0.1.0";

      src = pkgs.fetchFromGitHub {
        owner = "Thessal";
        repo = "butterflow";
        rev = "main";
        sha256 = "sha256-VyIRRzKju7BjTixQJQdUos78mC0duweYdI8U+i5ld6c=";
      };

      nativeBuildInputs = [ self.hatchling ];

      propagatedBuildInputs = [ 
        self.numpy
        self.scipy
        self.pytest
      ];

      pyproject = true;
    };
    morpho = self.buildPythonPackage rec {
      pname = "morpho";
      version = "0.1.0";

      src = pkgs.fetchFromGitHub {
        owner = "Thessal";
        repo = "transpiler";
        rev = "main";
        sha256 = "sha256-9Z8SqpvRRRxKNCtTpc7UBbdLgyOCnM8tM4A7IVOKmjU="; 
      };

      nativeBuildInputs = [ self.hatchling ];

      propagatedBuildInputs = [ 
        self.ollama
        self.chromadb
        self.butterflow
      ];

      pyproject = true;
    };
  };
}
