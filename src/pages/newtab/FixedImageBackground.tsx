import {
  Box,
  styled,
  CircularProgress,
  alpha as muiAlpha,
  useMediaQuery,
} from "@mui/material";
import React from "react";
import { useImmer } from "use-immer";
import bgImg from "@/assets/images/justHer.jpg";
import { useForageFile } from "@/hooks/api-localforage/useForageFile";
import { useThemeStore } from "@/hooks/store/useThemeStore";
import type { BoxProps, Theme } from "@mui/material";

export function FixedImageBackground(props: Props) {
  const { alpha, blur } = props;

  const containerRef = React.useRef<HTMLDivElement>(null);

  const smallScreen = useMediaQuery<Theme>((theme) => {
    return theme.breakpoints.up("sm");
  });

  const xsBgImgKey = useThemeStore((store) => store.xsBgImgKey);
  const smBgImgKey = useThemeStore((store) => store.smBgImgKey);
  const query = useForageFile(smallScreen ? smBgImgKey : xsBgImgKey);

  const [state, updateState] = useImmer({
    width: 0,
    height: 0,
  });

  React.useEffect(() => {
    const containerEl = containerRef.current;

    if (!(containerEl instanceof HTMLElement)) {
      return;
    }

    const observer = new ResizeObserver(([{ contentBoxSize }]) => {
      React.startTransition(() => {
        updateState((draft) => {
          const [size] = contentBoxSize;
          draft.width = size.inlineSize;
          draft.height = size.blockSize;
        });
      });
    });

    observer.observe(containerEl);

    return () => {
      observer.disconnect();
    };
  }, [updateState]);

  return (
    <Box
      ref={containerRef}
      position={"fixed"}
      zIndex={1}
      sx={{
        inset: `calc(${20 * (blur / 100)}px * -2)`,
        transition(theme) {
          return theme.transitions.create(["filter", "inset"]);
        },
        filter: `blur(${20 * (blur / 100)}px)`,
      }}
    >
      {/* Image */}
      {(() => {
        if (query.data) {
          return (
            <StyledImg
              src={URL.createObjectURL(query.data)}
              alt={query.data.name}
              onLoad={(evt) => {
                URL.revokeObjectURL(evt.currentTarget.src);
              }}
              width={state.width}
              height={state.height}
            />
          );
        }

        return (
          <StyledImg
            src={fallbackBgImg}
            alt="Background image"
            width={state.width}
            height={state.height}
          />
        );
      })()}

      {/* Mask */}
      <Box
        position={"absolute"}
        zIndex={3}
        bgcolor={muiAlpha("#000", alpha / 100)}
        sx={{
          inset: 0,
          transition(theme) {
            return theme.transitions.create(["background-color"]);
          },
        }}
      />

      {/* Spin Icon */}
      {query.isPending && (
        <CircularProgress
          size={24}
          sx={{
            position: "absolute",
            zIndex: 4,
            left: 24,
            bottom: 24,
          }}
        />
      )}
    </Box>
  );
}

type Props = BoxProps & {
  alpha: number;
  blur: number;
};

const StyledImg = styled("img")({
  position: "absolute",
  inset: 0,
  zIndex: 2,
  objectFit: "cover",
});

const fallbackBgImg = new URL(bgImg, import.meta.url).href;
