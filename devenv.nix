{
  pkgs,
  lib,
  config,
  inputs,
  ...
}: {
  packages = with pkgs; [
    anydesk
  ];

  languages.python = {
    enable = true;
    venv = {
      enable = true;
      requirements = builtins.readFile ./requirements.txt;
    };
  };

  dotenv.enable = true;
}
