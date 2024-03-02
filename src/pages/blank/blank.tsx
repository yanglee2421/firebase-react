import { Customer } from "@/components/shared/Customer";
import { ImageBackground } from "@/components/ui/ImageBackground";
import { useThemeStore } from "@/hooks/store/useThemeStore";
import React from "react";
import { Button } from "@mui/material";
import { Link } from "react-router-dom";

export function Blank() {
  const bgAlpha = useThemeStore((store) => store.bgAlpha);
  const bgBlur = useThemeStore((store) => store.bgBlur);
  const deferredAlpha = React.useDeferredValue(bgAlpha);
  const deferredBlur = React.useDeferredValue(bgBlur);

  return (
    <ImageBackground
      alpha={deferredAlpha}
      blur={deferredBlur}
      position={"fixed"}
      sx={{ inset: 0 }}
    >
      <Customer></Customer>
      <Link to={{ pathname: "/" }}>
        <Button variant="contained" size="large">
          Take me home
        </Button>
      </Link>
    </ImageBackground>
  );
}
